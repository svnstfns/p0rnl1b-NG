#!/bin/sh


# Exporter-Komponente
mkdir exporter
echo "# Hauptskript für den Exporter. Sendet File-Informationen an den Collector." > exporter/exporter.py
echo "# Exporter-Paketinitialisierung." > exporter/__init__.py

# Collector-Komponente
mkdir collector
echo "# Flask-Webserver, stellt die REST-API-Endpunkte bereit." > collector/webserver.py
echo "# Klassen und Funktionen für die Datenbankkommunikation und Verwaltung der Tabellen." > collector/database.py
echo "# Worker-Logik für asynchrone Aufgaben und Queue-Management." > collector/worker.py
echo "# Kontrolliert und verwaltet die Worker-Prozesse." > collector/worker_manager.py
echo "# Collector-Paketinitialisierung." > collector/__init__.py

# Gemeinsame Ressourcen
mkdir common
echo "# Gemeinsame Hilfsfunktionen und wiederverwendbare Module." > common/utils.py
echo "# Common-Paketinitialisierung." > common/__init__.py

# Tests
mkdir tests
echo "# Tests für die Exporter-Komponente." > tests/test_exporter.py
echo "# Tests für die Collector-Komponente." > tests/test_collector.py
echo "# Tests für gemeinsam genutzte Module." > tests/test_common.py
echo "# Paketinitialisierung für Tests." > tests/__init__.py

# Sphinx-Dokumentation vorbereiten
mkdir docs
echo "# Sphinx-Konfigurationsdatei. Hier wird die Dokumentation eingerichtet." > docs/conf.py
echo "# Hauptdatei für die Dokumentationsübersicht." > docs/index.rst

# Setup-Dateien für das Projekt
echo "# Projekt-Metadaten und Abhängigkeiten. Wird für die Installation verwendet." > setup.py
echo "# Anforderungen und Abhängigkeiten für das Projekt." > requirements.txt
echo "# Allgemeine Projektbeschreibung und Hinweise." > README.md
