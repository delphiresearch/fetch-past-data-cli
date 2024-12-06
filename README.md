# Setup

1. Create virtual environment

```
python3 -m venv venv
```

2. Activate virtual environment
   source venv/bin/activate

3. Install dependencies

```
pip install -r requirements.txt
```

4. Run the app

```
python app.py
```

# Fetch Event

```
python gamma/fetch-event/fetch_all_event.py
```

- Output: gamma/fetch-event/output/events.json

# Post Event to Supabase

- Input: gamma/fetch-event/output/events.json

```
python supabase/script.py
```
