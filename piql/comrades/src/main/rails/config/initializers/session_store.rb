# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_comrades_session',
  :secret      => '1a7d31fcdc65ac4c2c1b6003e91dda8626e510f8968c9f1ebddbee1b441be67418871cb0b74f54e76cd425d5724c6e54853c4f5b9334a858206c02a90a446dc6'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
