package bank

type LoanRequest struct {
	Ssn          int     `xml:"ssn" json:"ssn"`
	CreditScore  int     `xml:"creditScore" json:"creditScore"`
	LoanAmount   float64 `xml:"loanAmount" json:"loanAmount"`
	LoanDuration int     `xml:"loanDuration" json:"loanDuration"`
}

type LoanResponse struct {
	InterestRate float64 `xml:"interestRate" json:"interestRate"`
	Ssn          int     `xml:"ssn" json:"ssn"`
}
