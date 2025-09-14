"""
Upload form view for file upload API
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
import json
import logging

logger = logging.getLogger(__name__)


def upload_form_view(request):
    """
    Render the upload form HTML page
    """
    return render(request, 'core/upload_form.html')


@csrf_exempt
@require_http_methods(["POST"])
def upload_file_view(request):
    """
    Handle file upload via AJAX
    """
    try:
        # Get form data
        file = request.FILES.get('file')
        engagement_id = request.POST.get('engagement_id', '')
        client_name = request.POST.get('client_name', '')
        company_name = request.POST.get('company_name', '')
        fiscal_year = int(request.POST.get('fiscal_year', 2025))
        audit_start_date = request.POST.get('audit_start_date', '')
        audit_end_date = request.POST.get('audit_end_date', '')
        description = request.POST.get('description', '')
        
        # Validate required fields
        if not file:
            return JsonResponse({'error': 'No file provided'}, status=400)
        
        if not client_name:
            return JsonResponse({'error': 'Client name is required'}, status=400)
        
        if not company_name:
            return JsonResponse({'error': 'Company name is required'}, status=400)
        
        # Validate file type
        if not file.name.endswith('.csv'):
            return JsonResponse({'error': 'Only CSV files are supported'}, status=400)
        
        # Here you would call your existing upload API logic
        # For now, return a success response
        return JsonResponse({
            'message': 'File upload completed successfully',
            'status': 'success',
            'file_name': file.name,
            'file_size': file.size,
            'engagement_id': engagement_id,
            'client_name': client_name,
            'company_name': company_name,
            'fiscal_year': fiscal_year,
            'audit_start_date': audit_start_date,
            'audit_end_date': audit_end_date,
            'description': description
        })
        
    except Exception as e:
        logger.error(f"Error in upload_file_view: {e}")
        return JsonResponse({'error': str(e)}, status=500)
