# 1. The Base: Blank OS with Python 3.10
FROM python:3.10-slim

# 2. The Workspace: Create a folder inside the box
WORKDIR /app

# 3. The Copy: Move the dummy engine into the box
COPY contestant_engine.py /app/contestant_engine.py

# 4. The Port: Document the hole in the wall
EXPOSE 8080

# 5. The Command: Turn on the engine (Unbuffered mode)
CMD ["python", "-u", "/app/contestant_engine.py"]