package main

func main() {
	uri := "mongodb://localhost:27017"
	fp := NewFileProcessor()
	fp.Initiate(uri, "users", "users", "output.csv", 66, "insertMany")
	fp.StartProcessing()

}
