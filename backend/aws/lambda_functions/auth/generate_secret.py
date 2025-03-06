import secrets
import os
from dotenv import load_dotenv

def generate_jwt_secret():
    # Generate a secure random string
    new_secret = secrets.token_hex(16)
    print(f"Generated JWT Secret: {new_secret}")
    
    # Read current .env file
    load_dotenv()
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    
    with open(env_path, 'r') as file:
        lines = file.readlines()
    
    # Update JWT_SECRET line or add it if not present
    jwt_line_found = False
    for i, line in enumerate(lines):
        if line.startswith('JWT_SECRET='):
            lines[i] = f'JWT_SECRET={new_secret}\n'
            jwt_line_found = True
            break
    
    if not jwt_line_found:
        lines.append(f'JWT_SECRET={new_secret}\n')
    
    # Write back to .env
    with open(env_path, 'w') as file:
        file.writelines(lines)
    
    print("JWT Secret updated in .env file")

if __name__ == "__main__":
    generate_jwt_secret() 