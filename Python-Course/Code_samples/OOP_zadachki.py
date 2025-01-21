# Bank account

class BankAccount:
    def __init__(self, initial_balance):
        self.balance = initial_balance

    def deposit(self, amount):
        self.balance += amount
        print(f"Vnosim {amount}, balans stanovitsja {self.balance} ")

    def withdraw(self, amount):
        if self.balance >= amount:
            self.balance -= amount
            print(f"Zaberaem {amount}, balans stanovitsja {self.balance} ")
        else:
            print("Netu deneg")

    def check_balance(self):
        print(f"Ostalos na schetu: {self.balance}")


account = BankAccount(100)
account.deposit(50)
account.withdraw(100)
account.check_balance()
#account.balance()




#Library
#sozdat class library, kotoryj dolzhen sodershat info o knigah v biblioteke ...

from typing import List

class Library:
    def __init__(self, books:List):
        self.books = books

    def add_book(self, book):
        self.books.append(book)

    def remove_book(self, book):
        if book in self.books:
            self.books.remove(book)
        else:
            print("Book not found")

    def has_books (self, book):
        return book in self.books

    def list_books(self):
        print("Books in the library:")
        for book in self.books:
            print(f"- {book}")



library1 = Library(["Tri porosenka", "Bog"])
library1.list_books()


# Car

class Car:
    def __init__(self, brand, model, year, mileage):
        self.brand = brand
        self.model = model
        self.year = year
        self.mileage = mileage

    def add_mileage(self, km):
        self.mileage += km

    def get_info(self):
        print(f"Марка: {self.brand}, Модель: {self.model}, Год: {self.year}, Пробег: {self.mileage} км")


car1 = Car("Toyota", "Yaris", 2020, 100000)
car1.get_info()


# Book

class Book:
    def __init__(self, title, author, pages):
        self.title = title
        self.author = author
        self.pages = pages

    def get_description(self):
        return (f"Название: {self.title}, Автор: {self.author}, Страниц: {self.pages}")

book = Book("1984", "George Orwell", 328)

print(book.get_description())
print(Book.get_description(book))


# Library (classmethod)

class Library:
    total_books = 0

    def __init__(self):
        self.books = []

    def add_book(self, book):
        self.books.append(book)
        Library.total_books += 1

    def get_books(self):
        return [book1 for book1 in self.books]

    @classmethod
    def get_total_books(cls):
        return cls.total_books

    @classmethod
    def from_books_list(cls, books_list):
        library = cls()
        for book in books_list:
            library.add_book(book)
        return library


library1 = Library()
library1.add_book("1984")
library1.add_book("Brave New World")

library2 = Library.from_books_list(["The Great Gatsby", "To Kill a Mockingbird"])

print(library1.get_books())


# Upravlenije bibliotekoj

class Book:
    def __init__(self, name, author, ISBN, year, quantity):
        self.name = name
        self.author = author
        self.ISBN = ISBN
        self.year = year
        self.quantity = quantity
        Library.add_book(self)

    def get_quantity(self,n):
        self.quantity-= n

    def get_book_info (self):
        print(f"Info book :{self.name}, author: {self.author}, ISBN: {self.ISBN}, year: {self.year}, quantity: {self.quantity}")

class Reader:

    def __init__(self, r_number):
        self.r_number = r_number
        Library.add_reader(self)
        self.list_hold_books = []

    def take_book(self, book):
        self.list_hold_books.append(book)

    def return_book(self, book):
        self.list_hold_books.remove(book)

class Library:
    books = []
    readers= []

    @classmethod
    def add_book (cls, book):
        cls.books.append(book)

    @classmethod
    def add_reader (cls, reader):
        cls.readers.append(reader)

    @classmethod
    def remove_book (cls, name):
        for book in cls.books:
            if book.name == name:
                cls.books.remove(book)

    @classmethod
    def find_book (cls, name):
        for book in cls.books:
            if book.name == name:
                print (f"book: {book.name}, {book.author}, {book.ISBN}")



book1 = Book("A", "author1", 100, 2010, 1)
book2 = Book("B", "author2", 200, 2020, 2)
book3 = Book("C", "author3", 300, 2030, 3)

reader1 = Reader(1)
reader2 = Reader(2)

library1 = Library()


reader1.take_book(book1)
print([book.name for book in reader1.list_hold_books])


