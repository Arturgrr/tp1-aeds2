package storage

import "aeds2-tp1/entity"

type StorageStats struct {
	TotalBlocks       int
	TotalBytesUsed    int
	TotalBytesTotal   int
	EfficiencyRate    float64
	PartialBlocks     int
	BlockStatsList    []BlockStats
}

type BlockStats struct {
	BlockNumber   int
	BytesUsed     int
	BytesTotal    int
	OccupancyRate float64
	RecordsCount  int
}

type Storage interface {
	WriteStudents(filename string, students []entity.Student) error
	FindStudentByMatricula(filename string, matricula int) (*entity.Student, error)
	GetAllStudents(filename string) ([]*entity.Student, error)
	AddStudents(filename string, students []entity.Student) error
	GetStats(filename string) StorageStats
	ValidateBlockSize(blockSize int) error
	GetBlockSize() int
}
