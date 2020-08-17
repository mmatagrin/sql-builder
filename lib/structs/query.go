package structs

type Query struct {
	Select                  []string
	Delete 					string
	From                    string
	Join                    []Join
	Where                   []string
	WhereQueryParameters    []QueryParameter
	OrWhereQueryParameters  []QueryParameter
	OrWhere                 []string
	Order                   []OrderBy
	Having                  []string
	HavingQueryParameters   []QueryParameter
	HavingOrCondition       []string
	HavingOrQueryParameters []QueryParameter
	GroupBy                 []string
	Limit                   [2]int
	Union                   []Query
}
