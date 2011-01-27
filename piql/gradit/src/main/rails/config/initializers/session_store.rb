# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_Gradit_session',
  :secret      => '1da7711b32154788c20f73f9bc4c9c3e4e2684c2734e9766b8ef0a90650e2897be6a755d42ee3efacf4f31d7007c1381d7dedc0dc16da2d907e4106cdb0e8d25'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
