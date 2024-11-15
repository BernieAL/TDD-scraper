from werkzeug.security import generate_password_hash

# Generate hash
password = "admin!"  # Your desired password
hashed_password = generate_password_hash(password)
print(hashed_password)

# Then update your database with this hash
# It will look something like: 'scrypt:32768:8:1$salt$hash'