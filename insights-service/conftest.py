import os
import sys

# Les tests importent `analysis` et `app` depuis ce dossier ; le planificateur est désactivé.
sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("INSIGHTS_SCHEDULER", "false")
