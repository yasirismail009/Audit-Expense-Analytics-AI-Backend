from rest_framework import serializers
from .models import Expense, ExpenseSheet, SheetAnalysis, ExpenseAnalysis, AnalysisSession

class ExpenseSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExpenseSheet
        fields = '__all__'

class ExpenseSerializer(serializers.ModelSerializer):
    expense_sheet = ExpenseSheetSerializer(read_only=True)
    expense_sheet_id = serializers.IntegerField(write_only=True)
    
    class Meta:
        model = Expense
        fields = '__all__'
        extra_kwargs = {
            'expense_sheet': {'required': False}  # Allow setting via expense_sheet_id
        }

class SheetAnalysisSerializer(serializers.ModelSerializer):
    expense_sheet = ExpenseSheetSerializer(read_only=True)
    
    class Meta:
        model = SheetAnalysis
        fields = '__all__'

class ExpenseAnalysisSerializer(serializers.ModelSerializer):
    expense = ExpenseSerializer(read_only=True)
    sheet_analysis = SheetAnalysisSerializer(read_only=True)
    
    class Meta:
        model = ExpenseAnalysis
        fields = '__all__'

class AnalysisSessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalysisSession
        fields = '__all__' 