import os

from django.shortcuts import render_to_response
from django.shortcuts import RequestContext


def home(request):
    data = {'maps_api_key': os.getenv('GOOGLE_MAPS_API_KEY')}
    return render_to_response("homepage.djhtml", data, context_instance=RequestContext(request))
