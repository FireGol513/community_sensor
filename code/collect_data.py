#!/usr/bin/env python3
"""
Main data collection loop (daily CSV only).

Writes directly to:
    data/daily/<node_id>_YYYY-MM-DD.csv

Column order is defined by daily_writer.py (COLUMNS).
"""

from __future__ import annotations

import time
import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

# Importation des capteurs qu'on veut mesurer
from sensors.pms import PMSReader # Il y en a 2
from sensors.bme import read_bme
from sensors.so2 import init_so2, read_so2
# from sensors.opc_n3 import OPCN3

from utils.timekeeping import now_utc, utc_to_local, isoformat_utc_z, isoformat_local
from daily_writer import DailyWriter

def load_config(root: Path) -> Dict[str, Any]:
    """
    Récupération de la configuration du fichier node.yaml
    
    :param root: Répertoire racine du projet
    :type root: Path
    :return: Configuration sous forme de dictionnaire
    :rtype: Dict[str, Any]
    """
    cfg_path = root / "config" / "node.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def setup_logging(root: Path) -> logging.Logger:
    """
    Création d'un module pour faire les logs de la collecte de données
    
    :param root: Répertoire racine du projet
    :type root: Path
    :return: Logger configuré pour la collecte de données
    :rtype: Logger
    """
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "emis.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("emis.collect")


# -----------------------
# PMS agreement logic (PurpleAir-style)
# -----------------------
BASELINE_N = 30      # rolling baseline length (samples)
MIN_PM = 1.0         # below this, don't overreact to mismatch
RPD_OK = 0.25        # <= 25% relative percent difference = OK


def rpd(a: float, b: float) -> Optional[float]:
    """
    Calculer l'écart-type entre 2 nombres
    
    :param a: Nombre 1
    :type a: float
    :param b: Nombre 2
    :type b: float
    :return: Écart-type relatif entre a et b, ou None si non calculable
    :rtype: float | None
    """
    m = 0.5 * (a + b)
    if m <= 0:
        return None
    # ex: a=10, b=12 -> rpd=2/11=0.1818 
    # Donc écart-type relatif de 18.18%
    return abs(a - b) / m

def median(xs : list[float]) -> Optional[float]:
    """
    Calculer la médiane d'une liste de nombres
    
    :param xs: Liste de nombres
    :type xs: list[float]
    :return: Médiane des nombres, ou None si la liste est vide
    :rtype: float | None
    """
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    # Si n est impair, on prend le nombre du milieu
    # Si n est pair, on fait la moyenne des 2 nombres du milieu
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


def main() -> None:
    # Récupération du répertoire racine du projet et configuration du logger
    root = Path(__file__).resolve().parents[1]
    log = setup_logging(root)

    # Récupération de la config du fichier node.yaml
    cfg = load_config(root)
    node_id: str = cfg.get("node_id", "NodeX")
    tz_name: str = cfg.get("timezone", "UTC")
    tick_seconds: float = float(cfg.get("tick_seconds", 1.0)) # Délai entre chaque boucle de collecte de données

    s_cfg: Dict[str, Any] = cfg.get("sensors", {}) # Récupération de la configuration des capteurs s'il y en a

    # Rolling baselines for PMS diagnostics
    # Permet d'avoir une idée des valeurs habituelles des capteurs PMS pour détecter les anomalies (30 dernières mesures stockées)
    pms1_hist : deque[float] = deque(maxlen=BASELINE_N)
    pms2_hist : deque[float] = deque(maxlen=BASELINE_N)

    # -----------------------
    # Initialise sensors
    # -----------------------
    pms1_reader: Optional[PMSReader] = None
    pms2_reader: Optional[PMSReader] = None
    # opc_reader: Optional[OPCN3] = None

    # TODO: Faire une fonction pour les capteurs pms (répétition de code car connexion serial)
    
    ### PMS1
    # Récupération de la configuration du capteur PMS1
    p1 = s_cfg.get("pms1", {})

    # Instantier le lecteur PMS1 si activé dans la config
    if p1.get("enabled", False):
        port = p1.get("port")
        if port:
            pms1_reader = PMSReader(port)
            log.info(f"PMS1 enabled on {port}")
        else:
            log.warning("PMS1 enabled but no port provided; disabling")

    ### PMS2
    # Récupération de la configuration du capteur PMS2
    p2 = s_cfg.get("pms2", {})

    # Instantier le lecteur PMS2 si activé dans la config
    if p2.get("enabled", False):
        port = p2.get("port")
        if port:
            pms2_reader = PMSReader(port)
            log.info(f"PMS2 enabled on {port}")
        else:
            log.warning("PMS2 enabled but no port provided; disabling")

    # TODO: Faire une fonction pour les capteurs de BME688 et SO2 (répétition de code car connexion I2C)

    ### BME688
    # Récupération de la configuration du capteur BME688
    b_cfg = s_cfg.get("bme", {})

    # Instantier le lecteur BME688 si activé dans la config
    bme_enabled = b_cfg.get("enabled", False)
    bme_bus = int(b_cfg.get("i2c_bus", 1)) # FIXME: Vérifier le type "int" puisqu'on essaie de convertir un byte en int. Cela ne fait pas beaucoup de sens.
    bme_addr = b_cfg.get("address", 0x76)
    try:
        bme_addr = int(str(bme_addr), 0) # FIXME: Vérifier le type "int" puisqu'on essaie de convertir un byte en int. Cela ne fait pas beaucoup de sens.
    except Exception:
        bme_addr = 0x76 # TODO: Faire des constantes pour la default address du BME688

    ### SO2
    # Récupération de la configuration du capteur SO2
    so_cfg = s_cfg.get("so2", {})
    so2_enabled = so_cfg.get("enabled", False)
    # Instantier le lecteur SO2 si activé dans la config
    so2_bus = int(so_cfg.get("i2c_bus", 1)) # FIXME: Vérifier le type "int" puisqu'on essaie de convertir un byte en int. Cela ne fait pas beaucoup de sens.
    so2_addr = so_cfg.get("address", 0x74)
    try:
        so2_addr = int(str(so2_addr), 0) # FIXME: Vérifier le type "int" puisqu'on essaie de convertir un byte en int. Cela ne fait pas beaucoup de sens.
    except Exception:
        so2_addr = 0x74 # TODO: Faire des constantes pour la default address du capteur de SO2

    # Si le capteur de SO2 est activé, on l'initialise
    # TODO: On le mettre dans un fonction
    if so2_enabled:
        try:
            init_so2(bus=so2_bus, address=so2_addr)
            log.info(f"SO2 enabled on I2C bus {so2_bus}, addr 0x{so2_addr:02X}")
        except Exception as e:
            log.warning(f"Disabling SO2 after init failure: {e}")
            so2_enabled = False

    # Daily writer
    dw = DailyWriter(root_dir=root, node_id=node_id, tz_name=tz_name)
    log.info("Starting collection loop (daily CSV only)")

    # Main loop de la collecte de données des capteurs 
    try:
        while True:
            # Récupérer les données actuel
            t_utc = now_utc()
            t_local = utc_to_local(t_utc, tz_name)

            # Préparer la ligne de données à écrire en lui donnant les informations que nous avons déjà
            row: Dict[str, Any] = {
                "timestamp_utc": isoformat_utc_z(t_utc),
                "timestamp_local": isoformat_local(t_local),
                "node_id": node_id,
            }

            # ---- BME ----
            # Lire les données du capteur BME s'il est activé
            if bme_enabled:
                try:
                    b = read_bme(bus=bme_bus, address=bme_addr)
                    if b:
                        row["temp_c"] = b.get("temp_c")
                        row["rh_pct"] = b.get("rh_pct")
                        row["pressure_hpa"] = b.get("pressure_hpa")
                        row["voc_ohm"] = b.get("voc_ohm")
                        row["bme_status"] = "ok"
                    else:
                        row["bme_status"] = "no_data"
                except Exception as e:
                    row["bme_status"] = f"error:{e}"
                    log.warning(f"BME read error: {e}")

            # TODO: Faire une fonction pour la lacture des capteurs PMS (répétition de code car même type de données)

            # ---- PMS1 ----
            # Lire les données du capteur PMS1 s'il est activé
            if pms1_reader is not None:
                try:
                    s1 = pms1_reader.read()
                    if s1:
                        row["pm1_atm_pms1"] = s1.get("pm1")
                        row["pm25_atm_pms1"] = s1.get("pm25")
                        row["pm10_atm_pms1"] = s1.get("pm10")
                        row["pms1_status"] = "ok"
                    else:
                        row["pms1_status"] = "no_frame"
                except Exception as e:
                    row["pms1_status"] = f"error:{e}"
                    log.warning(f"PMS1 read error: {e}")

            # ---- PMS2 ----
            # Lire les données du capteur PMS2 s'il est activé
            if pms2_reader is not None:
                try:
                    s2 = pms2_reader.read()
                    if s2:
                        row["pm1_atm_pms2"] = s2.get("pm1")
                        row["pm25_atm_pms2"] = s2.get("pm25")
                        row["pm10_atm_pms2"] = s2.get("pm10")
                        row["pms2_status"] = "ok"
                    else:
                        row["pms2_status"] = "no_frame"
                except Exception as e:
                    row["pms2_status"] = f"error:{e}"
                    log.warning(f"PMS2 read error: {e}")

            # ---- PMS pair diagnostics (PM2.5) ----
            # TODO: Vérifier pourquoi on vérifie seulement les données du PM2.5 et pas les autres (PM1.0, PM10) 
            # pour savoir s'il existe une divergence entre capteurs
            row["pm25_pms_mean"] = "NODATA"
            row["pm25_pms_rpd"] = "NODATA"
            row["pm25_pair_flag"] = "NODATA"
            row["pm25_suspect_sensor"] = "NODATA"   # will be set to OK if nothing is suspect

            
            pm1 = row.get("pm25_atm_pms1")
            pm2 = row.get("pm25_atm_pms2")
            st1 = row.get("pms1_status", "")
            st2 = row.get("pms2_status", "")

            # Update rolling baselines from "ok" readings
            # Ajouter la nouvelle valeur valide à l'historique pour le calcul de la médiane
            # TODO: Faire une fonction pour éviter la répétition de code
            if st1 == "ok" and pm1 is not None:
                try:
                    pms1_hist.append(float(pm1))
                except Exception:
                    pass

            if st2 == "ok" and pm2 is not None:
                try:
                    pms2_hist.append(float(pm2))
                except Exception:
                    pass

            # Status-first logic
            # Déterminer si les 2 capteurs PMS sont d'accord sur les mesures de PM2.5 et déterminer si l'un des 2 capteurs est défectueux
            if st1 != "ok" and st2 != "ok":
                row["pm25_pair_flag"] = "BOTH_BAD"
                row["pm25_suspect_sensor"] = "BOTH"
            elif st1 != "ok":
                row["pm25_pair_flag"] = "PMS1_BAD"
                row["pm25_suspect_sensor"] = "PMS1"
            elif st2 != "ok":
                row["pm25_pair_flag"] = "PMS2_BAD"
                row["pm25_suspect_sensor"] = "PMS2"
            elif pm1 is not None and pm2 is not None:
                try:
                    # Les deux capteurs sont "ok", on peut donc comparer leurs valeurs pour voir si elles sont similaires
                    pm1f = float(pm1)
                    pm2f = float(pm2)

                    # Moyenne des données PM2.5 des 2 capteurs
                    mean_pm = 0.5 * (pm1f + pm2f)
                    row["pm25_pms_mean"] = mean_pm

                    # Comparaison des données PM2.5 des 2 capteurs
                    # Si la moyenne est suffisamment élevée, on calcule l'écart-type relatif (RPD)
                    if mean_pm >= MIN_PM:
                        # Calcul de l'écart-type relatif entre les 2 capteurs PMS
                        d = rpd(pm1f, pm2f)
                        row["pm25_pms_rpd"] = d

                        # Si l'écart-type relatif est dans la plage acceptable, on marque "OK"
                        if d is not None and d <= RPD_OK:
                            row["pm25_pair_flag"] = "OK"
                        else:
                            # Aussi non, vérifier quel capteur est en faute en comparant aux valeurs médianes historiques
                            b1 = median(list(pms1_hist))
                            b2 = median(list(pms2_hist))

                            # Écart-type relatif par rapport aux valeurs médianes historiques
                            dev1 = abs(pm1f - b1) / max(b1, MIN_PM) if b1 is not None else 0.0
                            dev2 = abs(pm2f - b2) / max(b2, MIN_PM) if b2 is not None else 0.0

                            row["pm25_pair_flag"] = "MISMATCH"

                            # Si l'écart-type relatif d'un capteur est 50% plus grand que l'autre, on marque le capteur comme suspect
                            if dev1 > dev2 * 1.5:
                                row["pm25_suspect_sensor"] = "PMS1"
                            elif dev2 > dev1 * 1.5:
                                row["pm25_suspect_sensor"] = "PMS2"
                            else:
                                row["pm25_suspect_sensor"] = "BOTH"
                    else:
                        row["pm25_pair_flag"] = "LOW_PM_OK"

                except Exception:
                    row["pm25_pair_flag"] = "ERROR"
                    row["pm25_suspect_sensor"] = "UNKNOWN"
            else:
                # Not enough data to compare (e.g., one PM missing but status ok)
                row["pm25_pair_flag"] = "INCOMPLETE"
                row["pm25_suspect_sensor"] = "UNKNOWN"

            # If nothing ended up being "suspect", explicitly mark OK.
            if row.get("pm25_suspect_sensor", "") == "" and row.get("pm25_pair_flag", "") in ("OK", "LOW_PM_OK"):
                row["pm25_suspect_sensor"] = "OK"
            
            # ---- SO2 ----
            # Si le capteur de SO2 est activé, on lit les données
            if so2_enabled:
                try:
                    v = read_so2()
                    row["so2_ppm"]   = v.get("so2_ppm")
                    row["so2_raw"]   = v.get("so2_raw")
                    row["so2_byte0"] = v.get("so2_byte0")
                    row["so2_byte1"] = v.get("so2_byte1")
                    row["so2_error"] = v.get("so2_error")     # "OK" if fine
                    row["so2_status"] = v.get("so2_status")   # "ok" or "error"
                except Exception as e:
                    row["so2_ppm"] = "NODATA"
                    row["so2_error"] = f"exception:{e}"
                    row["so2_status"] = "error"
                    log.warning(f"SO2 read error: {e}")


            # Write
            # Écrire la ligne de données dans le fichier CSV quotidien
            dw.write_sample(row=row, sample_time_utc=t_utc)
            time.sleep(tick_seconds)

    except KeyboardInterrupt:
        log.info("Stopping collection loop (KeyboardInterrupt)")

    finally:
        dw.close()
        if pms1_reader is not None:
            pms1_reader.close()
        if pms2_reader is not None:
            pms2_reader.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()