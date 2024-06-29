# Use the session to interact with the database
new_user = Domain(name='John Doe', email='john@example.com')
session.add(new_user)
session.commit()

# Query the database
users = session.query(Domain).all()
for user in users:
    print(f"Domain: {user.name}, Email: {user.email}")

# Close the session
session.close()