package structs

type QueryParameter struct {
	WhereConditions        []string
	OrWhereConditions      []string
	WhereFieldsSeparated   []QueryParameter
	OrWhereFieldsSeparated []QueryParameter
}
