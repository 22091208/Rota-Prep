# Team ROTA Generator

A Streamlit-based ROTA and change-support allocation application for team scheduling.

## Main features

- Create a rota with **names on rows** and **dates on columns**
- Capture and persist team member details:
  - Name
  - Dept.
  - File Id
  - Afternoon-only exception
- Persist inputs locally so they remain available after restart
- Enter leave ranges using **start date** and **end date**
- Configure bank holidays by:
  - no holidays
  - number of holiday days per month
  - specific dates
  - both
- Enforce scheduling rules:
  - minimum **2 Morning**, **2 Afternoon**, **2 Night** per day
  - maximum **5 continuous Night shifts**
  - compulsory **2 Week Off days after a Night block**
  - maximum **6 continuous working days**
  - same number of target week offs per member per month
- Afternoon-only rule:
  - weekday assignment only to Afternoon shift
  - **Saturday and Sunday automatically treated as Week Off**
- Sync 2 or more members to a primary member’s shift
- Save generated rota automatically for later viewing and allocation use
- Role-based access:
  - **Admin / Dev** can manage inputs and generate rota
  - **All users** can view the last generated rota and use change allocation
- Change support allocation:
  - select change start/end date and time in **GMT**
  - view available overlapping resources from the generated rota
  - maximum **3 resources per shift** allocated for a change

## Shift timings (GMT)

- Morning: **01:00 – 10:30**
- Afternoon: **07:30 – 17:00**
- Night: **15:30 – 01:00 next day**

## Files created by the app

The app stores data locally in the same folder as `app.py`:

- `rota_saved_inputs.json` — saved team inputs, leaves, bank holidays, sync groups
- `rota_saved_output.json` — last generated rota data
- `rota_saved_output.xlsx` — last generated rota in Excel format
- `rota_saved_matrix.csv` — last generated rota matrix in CSV format

## Project files

Recommended file names:

- `app.py` — main Streamlit application
- `requirements.txt` — Python dependencies
- `README.md` — project instructions

## Installation

### 1. Create and activate a virtual environment (recommended)

#### macOS / Linux

```bash
python3 -m venv rota-env
source rota-env/bin/activate
```

#### Windows

```bash
python -m venv rota-env
rota-env\Scripts\activate
```

### 2. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

## Running the app

Place your latest application file in the project folder as `app.py`, then run:

```bash
python3 -m streamlit run app.py
```

Streamlit usually opens the app at:

```text
http://localhost:8501
```

## Login / role access

The prototype uses simple hardcoded credentials inside the app code.

Typical behavior:

- **Admin / Dev**: full access to edit inputs and generate rota
- **General User**: read-only access to saved rota and change support allocation

Update the credentials in `app.py` before wider team use.

## How to use

### Admin / Dev flow

1. Open the app and log in as **Admin** or **Dev**
2. Enter or update team members
3. Enter leave ranges
4. Enter bank holidays or holiday count
5. Optionally define shift sync groups
6. Choose start date, end date, and week offs per month
7. Generate the rota
8. Review outputs in the available tabs
9. Download Excel or CSV if needed

### General user flow

1. Open the app as a general user
2. View the latest saved rota
3. Open the **Change Support Availability** tab
4. Enter change start and end date/time in GMT
5. Review allocated resources and their details

## Input format

### Team Members

Expected columns:

- `name`
- `dept`
- `file_id`
- `afternoon_only` (`Yes` or `No`)

### Leaves

Expected columns:

- `name`
- `leave_start_date`
- `leave_end_date`

### Specific Bank Holidays

Expected column:

- `bank_holiday_date`

### Shift Sync Groups

Expected column:

- `sync_group`

Example value:

```text
Aarav, Bhavna, Divya
```

This means:

- Aarav = primary member
- Bhavna and Divya will try to follow Aarav’s shift when possible

## Notes

- If staffing constraints, leaves, sync rules, or afternoon-only rules conflict, the app uses best-effort scheduling and logs warnings.
- Bank holidays do **not** reduce minimum staffing requirements.
- If the team is too small for the selected rules, warnings will appear in the **Warnings** tab.

## Dependencies

This app uses:

- Streamlit
- pandas
- openpyxl

## Troubleshooting

### `command not found: pip`
Use:

```bash
python3 -m pip install -r requirements.txt
```

### `command not found: streamlit`
Use:

```bash
python3 -m pip install streamlit
python3 -m streamlit run app.py
```

### Date column errors in Streamlit data editor
This happens when date columns are loaded as strings. Use the latest `app.py` version, which converts saved date fields properly before rendering.
