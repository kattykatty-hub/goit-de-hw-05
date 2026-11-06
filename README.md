#GoIT Data Engineering HW05 — Kafka IoT Monitoring
#Project files
- `configs.py`: broker connection via local `.env` and topic names
- `create_topics.py`: create/list 3 topics
- `sensor_producer.py`: one sensor per running process
- `sensor_processor.py`: filter readings and send alerts
- `alert_consumer.py`: receive both alert streams
- `screenshots/`: capture the five required evidence screenshots
- `docs/`: complete the execution report with actual screenshots

#Setup (PowerShell, Windows)
```powershell
cd C:\Users\PC\Desktop\goit-de-hw-05
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
notepad .env
```

#Run
```powershell
python create_topics.py
```
Open four separate terminals, activate the venv in each, and run in order:
```powershell
python alert_consumer.py
python sensor_processor.py
python sensor_producer.py --sensor-id 101 --building-id building_A
python sensor_producer.py --sensor-id 202 --building-id building_B
```
Each command above runs in its **own** terminal. Stop with Ctrl+C.

#Rules
- `temperature > 40` → `katty_temperature_alerts`
- `humidity > 80 or humidity < 20` → `katty_humidity_alerts`
- Normal readings generate no alerts.


