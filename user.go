package main

type User struct {
	Name  string
	Age   int
	Email string
}

func NewUser(name string, age int, email string) User {
	return User{name, age, email}
}
