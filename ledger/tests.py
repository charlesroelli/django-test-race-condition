import threading

from django.contrib.auth.models import User
from django.db import connection, transaction
from django.db.models.aggregates import Sum
from django.db.utils import OperationalError
from django.test import TransactionTestCase
from psycopg.errors import SerializationFailure

from ledger.models import Ledger

def balance(user_id: int) -> int:
    return Ledger.objects.filter(user_id=1).aggregate(balance=Sum("amount"))["balance"]

def withdraw(user_id: int, amount: int) -> None:
    with transaction.atomic():
        if balance(user_id) >= amount:
            Ledger.objects.create(user_id=1, amount=-amount)
        else:
            raise ValueError("Insufficient balance")


class LedgerTests(TransactionTestCase):
    def test_ledger_race_condition(self):
        User.objects.create(pk=1, username="foo")
        Ledger.objects.create(user_id=1, amount=500)
        barrier = threading.Barrier(2)

        def concurrent_transaction():
            with transaction.atomic():
                withdraw(user_id=1, amount=500)
                barrier.wait()
            connection.close()

        thread_one = threading.Thread(target=concurrent_transaction)
        thread_one.start()
        thread_two = threading.Thread(target=concurrent_transaction)
        thread_two.start()
        thread_one.join()
        thread_two.join()

        # Confirm that we withdrew more money than the user had
        self.assertEqual(balance(user_id=1), -500)

    def test_ledger_concurrency_safe(self):
        User.objects.create(pk=1, username="foo")
        Ledger.objects.create(user_id=1, amount=500)
        barrier = threading.Barrier(2)

        def concurrent_transaction():
            try:
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                        )
                    withdraw(user_id=1, amount=500)
                    party = barrier.wait()
            except OperationalError as error:
                print(f"{party=}")
                self.assertIsInstance(error.__cause__, SerializationFailure)
            finally:
                connection.close()

        thread_one = threading.Thread(target=concurrent_transaction)
        thread_one.start()
        thread_two = threading.Thread(target=concurrent_transaction)
        thread_two.start()
        thread_one.join()
        thread_two.join()

        # Confirm there was no overdraft
        self.assertEqual(balance(user_id=1), 0)
