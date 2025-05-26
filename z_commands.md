# Install UV
curl -Ls https://astral.sh/uv/install.sh | sh

# Create and activate venv
uv venv
source .venv/bin/activate 

# UV
curl -Ls https://astral.sh/uv/install.sh | sh

uv pip install -r requirements.txt

# Install FastAPI and the production-ready uvicorn[standard] server
uv pip install fastapi
uv pip install "uvicorn[standard]"

# If you're using uv, it's safe to rely on .lock.txt instead of requirements.txt — or keep both if you want.
uv pip freeze > requirements.lock.txt

# Launch FastAPI
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
