package aurora

import (
	structs "github.com/mmatagrin/sql-builder/structs"
)

type AuroraQueryParameter struct {
	Parameters structs.QueryParameter
}

func (mqp *AuroraQueryParameter) Where(expression string) *AuroraQueryParameter {
	mqp.Parameters.WhereConditions = append(mqp.Parameters.WhereConditions, expression)
	return mqp
}

func (mqp *AuroraQueryParameter) OrWhere(expression string) *AuroraQueryParameter {
	mqp.Parameters.OrWhereConditions = append(mqp.Parameters.OrWhereConditions, expression)
	return mqp
}

func (mqp *AuroraQueryParameter) WhereFunc(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryParameter {
	mqp.Parameters.WhereFieldsSeparated = append(mqp.Parameters.WhereFieldsSeparated, callback(AuroraQueryParameter{}).Parameters)
	return mqp
}

func (mqp *AuroraQueryParameter) OrWhereFunc(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryParameter {
	mqp.Parameters.OrWhereFieldsSeparated = append(mqp.Parameters.OrWhereFieldsSeparated, callback(AuroraQueryParameter{}).Parameters)
	return mqp
}
