# FastAPI Supabase POC

## Setup on Coolify
1. Create a Coolify app, point to this repo.
2. Add environment variables: `SUPABASE_URL`, `SUPABASE_JWT_SECRET`, `DATABASE_URL`.
3. Set build command: `pip install -r requirements.txt`
4. Set run command: `uvicorn app.main:app --host 0.0.0.0 --port 80`
5. Deploy.
