from django.contrib import admin
from .models import DatasetService, SpatialDatasetService


class DatasetServiceAdmin(admin.ModelAdmin):
    """
    Admin model for Web Processing Service Model
    """
    fields = ('name', 'engine', 'endpoint', 'apikey', 'username', 'password')


class SpatialDatasetServiceAdmin(admin.ModelAdmin):
    """
    Admin model for Spatial Dataset Service Model
    """
    fields = ('name', 'engine', 'endpoint', 'apikey', 'username', 'password')


admin.site.register(DatasetService, DatasetServiceAdmin)
admin.site.register(SpatialDatasetService, SpatialDatasetServiceAdmin)
