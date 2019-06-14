package model

//Word filter struct
type WordFilter struct {
	ExcludeList  []string `json:"exclude_list"`
	MinFrequency int      `json:"min_frequency"`
	MinLen       int      `json:"min_len"`
}

//Exclude list getter
func (wf WordFilter) GetExcludeList() []string {
	return wf.ExcludeList
}

//Word min frequency getter
func (wf WordFilter) GetMinFrequency() int {
	return wf.MinFrequency
}

//Word min length getter
func (wf WordFilter) GetMinLen() int {
	return wf.MinLen
}

//Job structure
type Job struct {
	Url    string     `json:"url"`
	Filter WordFilter `json:"word_filter"`
}

//Url Getter
func (j *Job) GetURL() string {
	return j.Url
}

//Filter Getter
func (j *Job) GetFilter() WordFilter {
	return j.Filter
}
