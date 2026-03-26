# Team ROTA Generator

A simple Streamlit application to generate a dynamic team rota.

## What this app does
- Accepts team members and their weekly off days
- Accepts leave dates for each team member
- Supports an **exception** flag for people who should always stay in **Afternoon** shift
- Allocates shifts dynamically even when team size changes
- Tries to distribute **Morning** and **Night** shifts evenly
- Keeps **2 resources in Morning** and **2 resources in Night** every day
- Puts the remaining available team members in **Afternoon** shift
- Enforces **2 rest days after a Night shift** before the next assignment
- Flags days where staffing is not possible due to low headcount / too many leaves / too many exceptions

## Shift Rules Implemented
- Morning shift: exactly 2 resources
- Night shift: exactly 2 resources
- Afternoon shift: everyone else available
- Week off and leave are respected
- After Night shift, the employee gets **2 days of Post-Night Rest** by default
- Members marked as `afternoon_only = Yes` are never assigned Morning or Night

## Files
- `app.py` → main Streamlit app
- `requirements.txt` → dependencies
- `README.md` → run instructions

## How to run
1. Make sure Python 3.10+ is installed.
2. Open terminal in this folder.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run the app:

```bash
streamlit run app.py
```

5. The browser will open automatically. If not, copy the local URL shown in terminal.

## Input format inside the app
### Team Members table
- `name` → employee name
- `week_off_days` → one or more weekday names separated by commas
  - Example: `Sunday`
  - Example: `Saturday,Sunday`
- `afternoon_only` → `Yes` or `No`

### Leaves table
- `name` → employee name
- `leave_date` → leave date

## Output
The app generates:
- **Day-wise Schedule**
- **Matrix View** (names vs dates)
- **Summary** (Morning/Night/Afternoon counts)
- **Warnings** for understaffed days
- Download as **Excel** or **CSV**

## Important note
If your team size is too small, or too many people are on leave / fixed to Afternoon, the app will still generate the rota but will show warnings wherever Morning or Night cannot be fully staffed.

## Suggested next improvements
- Add login and role-based access
- Save rota history in database
- Manual override of assignments
- Public holiday support
- Skill-based assignment rules
- Export to PDF
