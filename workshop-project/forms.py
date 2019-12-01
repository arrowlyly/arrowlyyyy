from django import forms


class SearchForm(forms.Form):
    search = forms.CharField(label=False, required=True, min_length=1, max_length=200,
                             widget=forms.TextInput())
