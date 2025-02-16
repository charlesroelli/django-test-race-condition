from django.contrib.auth.models import User
from django.db import models

# Create your models here.

class Ledger(models.Model):
    amount = models.IntegerField()
    user = models.ForeignKey(User, on_delete=models.DO_NOTHING)
