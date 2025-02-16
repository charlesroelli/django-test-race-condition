import threading

from django.contrib.auth.models import User
from django.db import connection, transaction
from django.db.models.aggregates import Sum
from django.db.utils import OperationalError
from django.test import TransactionTestCase
from psycopg.errors import SerializationFailure

from ledger.models import Ledger

# Create your tests here.

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

        # Use an event to control the order in which the transactions
        # will commit and make sure they commit at the same time.
        waiter = threading.Event()

        def _concurrent_transaction():
            with transaction.atomic():
                withdraw(user_id=1, amount=500)
                waiter.set()
            connection.cursor().close()

        # Start a transaction in a separate thread
        thread = threading.Thread(target=_concurrent_transaction)
        thread.start()

        # Start another transaction and signal the threaded
        # one to commit at the same time.
        with transaction.atomic():
            withdraw(user_id=1, amount=500)
            waiter.wait()

        # Wait for the concurrent transaction to finish/commit
        thread.join()

        # Confirm that we withdrew more money than the user had
        self.assertEqual(balance(user_id=1), -500)

    def test_ledger_concurrency_safe(self):
        User.objects.create(pk=1, username="foo")
        Ledger.objects.create(user_id=1, amount=500)

        # Use an even to control the order in which the transactions
        # will commit and make sure they commit at the same time.
        waiter = threading.Event()

        def _concurrent_transaction():
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                withdraw(user_id=1, amount=500)
                waiter.set()

        # Start a transaction in a separate thread
        thread = threading.Thread(target=_concurrent_transaction)
        thread.start()

        # Start another transaction and signal the threaded
        # one to commit at the same time.
        # with self.assertRaises(SerializationFailure) as e:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            withdraw(user_id=1, amount=500)
            waiter.wait()

        # Assert that the transaction was aborted due to a serialization failure
        # assert isinstance(e.exception.__cause__, psycopg2.errors.SerializationFailure)

        # Wait for the concurrent transaction to finish/commit
        thread.join()

        # Confirm there was no overdraft
        self.assertEqual(balance(user_id=1), 0)
