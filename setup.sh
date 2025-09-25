# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the required packages
pip install -r requirements.txt

# For Linux/macOS
export APP_SECRET_KEY='a-very-strong-and-random-secret-key-for-jwt'
export APP_ENCRYPTION_KEY='$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")'

# For Windows (Command Prompt)
set APP_SECRET_KEY="a-very-strong-and-random-secret-key-for-jwt"
# Note: On CMD, generate the encryption key separately and set it

python server.py
