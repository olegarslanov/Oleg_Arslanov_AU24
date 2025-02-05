
# Encapsulation (public, protected, private)

class Person:
    def __init__(self, name, age):
        self.__name = name
        self.__age = age

    def get_name(self):
        print(self.__name)

person = Person('Oleg', 46)
try:
    person.__name
except AttributeError:
    print(f"Privatnaja zapis person.__name")   #oshibka ne daet

print(f"Privatnaja zapis cherez ne hochiu {person._Person__name}")  #mozhno poluchit vse zhe dannyje napriamuju iz egzempliara s pomoschju vot tak
person.get_name()



class Person:
    def __init__(self, name, age):
        self._name = name
        self._age = age

    def get_name(self):
        print(self._name)

person = Person('Oleg', 46)
print(f" eto zashishenaja zapis: {person._name}")
person.get_name()

print(person.__name__)

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def get_name(self):
        print(self.name)

person = Person('Oleg', 46)
print(f" eto prostaja zapis: {person.name}")
person.get_name()


# Bank account

class BankAccount:
    def __init__(self, accountNumber, balance):
        self._accountNumber = accountNumber
        self._balance = balance

    def deposit_amount(self, amount):
        self._balance += amount
        return self._balance

    def withdraw_amount(self, amount):
        self._balance -= amount

    def get_balance(self):
        print(self._balance)

bank_account1 = BankAccount(100,0)
print(dir(bank_account1))

#print(bank_account1)
#print(bank_account1._balance)
#print(bank_account1.deposit_amount(100))
#print(bank_account1._balance)
#bank_account1.get_balance()