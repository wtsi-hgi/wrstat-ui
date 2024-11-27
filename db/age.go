package db

const (
	SecondsInAMonth = 2628000
	SecondsInAYear  = SecondsInAMonth * 12
	ErrInvalidAge   = Error("not a valid age")
)

var AgeThresholds = [8]int64{ //nolint:gochecknoglobals
	SecondsInAMonth, SecondsInAMonth * 2, SecondsInAMonth * 6, SecondsInAYear,
	SecondsInAYear * 2, SecondsInAYear * 3, SecondsInAYear * 5, SecondsInAYear * 7,
}

// DirGUTAge is one of the age types that the
// directory,group,user,filetype,age summaries group on. All is for files of
// all ages. The AgeA* consider age according to access time. The AgeM* consider
// age according to modify time. The *\dM ones are age in the number of months,
// and the *\dY ones are in number of years.
type DirGUTAge uint8

const (
	DGUTAgeAll DirGUTAge = 0
	DGUTAgeA1M DirGUTAge = 1
	DGUTAgeA2M DirGUTAge = 2
	DGUTAgeA6M DirGUTAge = 3
	DGUTAgeA1Y DirGUTAge = 4
	DGUTAgeA2Y DirGUTAge = 5
	DGUTAgeA3Y DirGUTAge = 6
	DGUTAgeA5Y DirGUTAge = 7
	DGUTAgeA7Y DirGUTAge = 8
	DGUTAgeM1M DirGUTAge = 9
	DGUTAgeM2M DirGUTAge = 10
	DGUTAgeM6M DirGUTAge = 11
	DGUTAgeM1Y DirGUTAge = 12
	DGUTAgeM2Y DirGUTAge = 13
	DGUTAgeM3Y DirGUTAge = 14
	DGUTAgeM5Y DirGUTAge = 15
	DGUTAgeM7Y DirGUTAge = 16
)

var DirGUTAges = [17]DirGUTAge{ //nolint:gochecknoglobals
	DGUTAgeAll, DGUTAgeA1M, DGUTAgeA2M, DGUTAgeA6M, DGUTAgeA1Y,
	DGUTAgeA2Y, DGUTAgeA3Y, DGUTAgeA5Y, DGUTAgeA7Y, DGUTAgeM1M,
	DGUTAgeM2M, DGUTAgeM6M, DGUTAgeM1Y, DGUTAgeM2Y, DGUTAgeM3Y,
	DGUTAgeM5Y, DGUTAgeM7Y,
}

// AgeStringToDirGUTAge converts the String() representation of a DirGUTAge
// back in to a DirGUTAge. Errors if an invalid string supplied.
func AgeStringToDirGUTAge(age string) (DirGUTAge, error) {
	convert := map[string]DirGUTAge{
		"0":  DGUTAgeAll,
		"1":  DGUTAgeA1M,
		"2":  DGUTAgeA2M,
		"3":  DGUTAgeA6M,
		"4":  DGUTAgeA1Y,
		"5":  DGUTAgeA2Y,
		"6":  DGUTAgeA3Y,
		"7":  DGUTAgeA5Y,
		"8":  DGUTAgeA7Y,
		"9":  DGUTAgeM1M,
		"10": DGUTAgeM2M,
		"11": DGUTAgeM6M,
		"12": DGUTAgeM1Y,
		"13": DGUTAgeM2Y,
		"14": DGUTAgeM3Y,
		"15": DGUTAgeM5Y,
		"16": DGUTAgeM7Y,
	}

	dgage, ok := convert[age]

	if !ok {
		return DGUTAgeAll, ErrInvalidAge
	}

	return dgage, nil
}
