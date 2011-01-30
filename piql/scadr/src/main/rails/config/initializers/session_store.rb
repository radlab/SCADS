# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_scadr_session',
  :secret      => '7d6c2303615c5cc5ce323ad1c32837639ad6988b9b57c6a288e66c669a212d4d1b13220e9ea869837b4eb75c40bc771368e0d88cfae594242254fcbd8c6aa805'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
