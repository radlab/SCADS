# Methods added to this helper will be available to all templates in the application.
module ApplicationHelper
	def csrf_meta_tag
	  if protect_against_forgery?
		out = %(<meta name="csrf-param" content="%s"/>\n)
		out << %(<meta name="csrf-token" content="%s"/>)
		out % [ Rack::Utils.escape_html(request_forgery_protection_token),
				Rack::Utils.escape_html(form_authenticity_token) ]
	  end
	end
end