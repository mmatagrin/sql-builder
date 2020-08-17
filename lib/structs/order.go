package structs

const (
	ASC  = 0
	DESC = 1
)

type Order int

type OrderBy struct {
	Field string
	Order Order
}

func (o Order) ToString() string {
	if o == 0 {
		return "ASC"
	} else {
		return "DESC"
	}
}

func JoinOrderBy(orders []OrderBy, separator string) string {
	var returnStr string
	for _, order := range orders {
		returnStr += order.Field + " " + order.Order.ToString() + separator
	}
	return returnStr[:len(returnStr)-len(separator)]
}
