package storage

import (
	"aeds2-tp1/entity"
	"encoding/binary"
	"fmt"
	"os"
)

type VariableStorage struct {
	blockSize int
	stats      StorageStats
}

func NewVariableStorage(blockSize int) (*VariableStorage, error) {
	vs := &VariableStorage{
		blockSize: blockSize,
		stats: StorageStats{
			BlockStatsList: make([]BlockStats, 0),
		},
	}
	
	if err := vs.ValidateBlockSize(blockSize); err != nil {
		return nil, err
	}
	
	return vs, nil
}

func (vs *VariableStorage) ValidateBlockSize(blockSize int) error {
	minSize := 4 +
		4 + entity.MaxNomeLength +
		entity.CPFLength +
		4 + entity.MaxCursoLength +
		4 + entity.MaxFiliacaoLength +
		4 + entity.MaxFiliacaoLength +
		4 +
		8
	
	if blockSize < minSize {
		return fmt.Errorf("tamanho do bloco (%d bytes) é menor que o tamanho mínimo necessário para um registro variável (%d bytes)", blockSize, minSize)
	}
	
	return nil
}

func (vs *VariableStorage) WriteStudents(filename string, students []entity.Student) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo: %w", err)
	}
	defer file.Close()

	currentBlock := make([]byte, 0, vs.blockSize)
	currentBlockNumber := 0
	blockStats := BlockStats{
		BlockNumber: currentBlockNumber,
		BytesUsed:   0,
		BytesTotal:  vs.blockSize,
	}

	for i, student := range students {
		recordData := vs.serializeStudent(student)
		
		if len(recordData) > vs.blockSize {
			return fmt.Errorf("registro do aluno %d (matrícula: %d) excede o tamanho do bloco (%d bytes > %d bytes). Aumente o tamanho do bloco", i+1, student.Matricula, len(recordData), vs.blockSize)
		}
		
		vs.writeContiguousRecord(&currentBlock, &currentBlockNumber, &blockStats, recordData, file, i == len(students)-1)
	}

	if len(currentBlock) > 0 {
		blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
		vs.stats.BlockStatsList = append(vs.stats.BlockStatsList, blockStats)
		vs.writeBlock(file, currentBlock)
		vs.stats.TotalBlocks++
		vs.stats.TotalBytesUsed += blockStats.BytesUsed
		vs.stats.TotalBytesTotal += blockStats.BytesTotal
	}

	vs.calculateFinalStats()
	return nil
}

func (vs *VariableStorage) serializeStudent(student entity.Student) []byte {
	data := make([]byte, 0)
	
	matriculaBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(matriculaBytes, uint32(student.Matricula))
	data = append(data, matriculaBytes...)
	
	nomeBytes := []byte(student.Nome)
	nomeLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(nomeLen, uint32(len(nomeBytes)))
	data = append(data, nomeLen...)
	data = append(data, nomeBytes...)
	
	cpfBytes := []byte(student.CPF)
	data = append(data, cpfBytes...)
	
	cursoBytes := []byte(student.Curso)
	cursoLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(cursoLen, uint32(len(cursoBytes)))
	data = append(data, cursoLen...)
	data = append(data, cursoBytes...)
	
	maeBytes := []byte(student.FiliacaoMae)
	maeLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(maeLen, uint32(len(maeBytes)))
	data = append(data, maeLen...)
	data = append(data, maeBytes...)
	
	paiBytes := []byte(student.FiliacaoPai)
	paiLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(paiLen, uint32(len(paiBytes)))
	data = append(data, paiLen...)
	data = append(data, paiBytes...)
	
	anoBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(anoBytes, uint32(student.AnoIngresso))
	data = append(data, anoBytes...)
	
	caBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(caBytes, uint64(student.CA*100))
	data = append(data, caBytes...)
	
	return data
}

func (vs *VariableStorage) writeContiguousRecord(currentBlock *[]byte, currentBlockNumber *int, blockStats *BlockStats, recordData []byte, file *os.File, isLast bool) {
	recordSize := len(recordData)
	
	if len(*currentBlock)+recordSize > vs.blockSize {
		if len(*currentBlock) > 0 {
			blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
			if blockStats.OccupancyRate < 100 {
				vs.stats.PartialBlocks++
			}
			vs.stats.BlockStatsList = append(vs.stats.BlockStatsList, *blockStats)
			vs.writeBlock(file, *currentBlock)
			vs.stats.TotalBlocks++
			vs.stats.TotalBytesUsed += blockStats.BytesUsed
			vs.stats.TotalBytesTotal += blockStats.BytesTotal
		}
		
		*currentBlock = make([]byte, 0, vs.blockSize)
		*currentBlockNumber++
		*blockStats = BlockStats{
			BlockNumber: *currentBlockNumber,
			BytesUsed:   0,
			BytesTotal:  vs.blockSize,
		}
	}
	
	*currentBlock = append(*currentBlock, recordData...)
	blockStats.BytesUsed += recordSize
	blockStats.RecordsCount++
}

func (vs *VariableStorage) writeBlock(file *os.File, block []byte) {
	paddedBlock := make([]byte, vs.blockSize)
	copy(paddedBlock, block)
	file.Write(paddedBlock)
}

func (vs *VariableStorage) calculateFinalStats() {
	if vs.stats.TotalBytesTotal > 0 {
		vs.stats.EfficiencyRate = float64(vs.stats.TotalBytesUsed) / float64(vs.stats.TotalBytesTotal) * 100
	}
}

func (vs *VariableStorage) GetStats(filename string) StorageStats {
	vs.recalculateStatsFromFile(filename)
	return vs.stats
}

func (vs *VariableStorage) recalculateStatsFromFile(filename string) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return
	}

	totalBlocks := int(fileInfo.Size()) / vs.blockSize
	vs.stats = StorageStats{
		TotalBlocks:     totalBlocks,
		TotalBytesTotal: totalBlocks * vs.blockSize,
		BlockStatsList:  make([]BlockStats, 0),
	}

	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	totalUsed := 0
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vs.blockSize))
		if err != nil {
			continue
		}

		bytesUsed := 0
		recordsCount := 0
		offset := 0
		for offset < vs.blockSize {
			if offset+4 > vs.blockSize {
				break
			}

			student, err := vs.deserializeStudentFromBlock(block, offset)
			if err != nil {
				break
			}

			recordSize := vs.getRecordSize(student)
			bytesUsed += recordSize
			recordsCount++
			offset += recordSize
		}

		totalUsed += bytesUsed

		occupancyRate := float64(bytesUsed) / float64(vs.blockSize) * 100
		blockStats := BlockStats{
			BlockNumber:   blockNum,
			BytesUsed:     bytesUsed,
			BytesTotal:    vs.blockSize,
			OccupancyRate: occupancyRate,
			RecordsCount:  recordsCount,
		}

		if occupancyRate < 100 && occupancyRate > 0 {
			vs.stats.PartialBlocks++
		}

		vs.stats.BlockStatsList = append(vs.stats.BlockStatsList, blockStats)
	}

	vs.stats.TotalBytesUsed = totalUsed
	if vs.stats.TotalBytesTotal > 0 {
		vs.stats.EfficiencyRate = float64(vs.stats.TotalBytesUsed) / float64(vs.stats.TotalBytesTotal) * 100
	}
}

func (vs *VariableStorage) FindStudentByMatricula(filename string, matricula int) (*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / vs.blockSize
	return vs.findStudentContiguous(file, totalBlocks, matricula)
}

func (vs *VariableStorage) findStudentContiguous(file *os.File, totalBlocks int, matricula int) (*entity.Student, error) {
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset < vs.blockSize {
			if offset+4 > vs.blockSize {
				break
			}

			matriculaBytes := block[offset : offset+4]
			readMatricula := int(binary.LittleEndian.Uint32(matriculaBytes))
			
			if readMatricula == matricula {
				student, err := vs.deserializeStudentFromBlock(block, offset)
				if err == nil {
					return student, nil
				}
			}

			student, err := vs.deserializeStudentFromBlock(block, offset)
			if err != nil {
				break
			}
			offset += vs.getRecordSize(student)
		}
	}

	return nil, fmt.Errorf("aluno com matrícula %d não encontrado", matricula)
}

func (vs *VariableStorage) deserializeStudentFromBlock(block []byte, offset int) (*entity.Student, error) {
	if offset+4 > len(block) {
		return nil, fmt.Errorf("offset fora dos limites")
	}

	recordData := block[offset:]
	student, err := vs.deserializeStudent(recordData)
	if err != nil {
		return nil, err
	}

	return student, nil
}

func (vs *VariableStorage) getRecordSize(student *entity.Student) int {
	size := 4
	size += 4 + len(student.Nome)
	size += 11
	size += 4 + len(student.Curso)
	size += 4 + len(student.FiliacaoMae)
	size += 4 + len(student.FiliacaoPai)
	size += 4
	size += 8
	return size
}

func (vs *VariableStorage) deserializeStudent(data []byte) (*entity.Student, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("dados insuficientes")
	}

	offset := 0

	matricula := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+4 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para nome")
	}
	nomeLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+nomeLen > len(data) {
		return nil, fmt.Errorf("dados insuficientes para nome")
	}
	nome := string(data[offset : offset+nomeLen])
	offset += nomeLen

	if offset+11 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para CPF")
	}
	cpf := string(data[offset : offset+11])
	offset += 11

	if offset+4 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para curso")
	}
	cursoLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+cursoLen > len(data) {
		return nil, fmt.Errorf("dados insuficientes para curso")
	}
	curso := string(data[offset : offset+cursoLen])
	offset += cursoLen

	if offset+4 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para filiação mãe")
	}
	maeLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+maeLen > len(data) {
		return nil, fmt.Errorf("dados insuficientes para filiação mãe")
	}
	filiacaoMae := string(data[offset : offset+maeLen])
	offset += maeLen

	if offset+4 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para filiação pai")
	}
	paiLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+paiLen > len(data) {
		return nil, fmt.Errorf("dados insuficientes para filiação pai")
	}
	filiacaoPai := string(data[offset : offset+paiLen])
	offset += paiLen

	if offset+4 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para ano de ingresso")
	}
	anoIngresso := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+8 > len(data) {
		return nil, fmt.Errorf("dados insuficientes para CA")
	}
	caValue := binary.LittleEndian.Uint64(data[offset : offset+8])
	ca := float64(caValue) / 100.0

	student := &entity.Student{
		Matricula:   matricula,
		Nome:        nome,
		CPF:         cpf,
		Curso:       curso,
		FiliacaoMae: filiacaoMae,
		FiliacaoPai: filiacaoPai,
		AnoIngresso: anoIngresso,
		CA:          ca,
	}

	student.TruncateFields()

	if err := student.Validate(); err != nil {
		return nil, fmt.Errorf("estudante deserializado inválido: %w", err)
	}

	return student, nil
}

func (vs *VariableStorage) GetBlockSize() int {
	return vs.blockSize
}

func (vs *VariableStorage) GetAllStudents(filename string) ([]*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / vs.blockSize
	students := make([]*entity.Student, 0)

	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset < vs.blockSize {
			if offset+4 > vs.blockSize {
				break
			}

			student, err := vs.deserializeStudentFromBlock(block, offset)
			if err != nil {
				break
			}
			
			if student.Matricula > 0 {
				students = append(students, student)
			}
			
			offset += vs.getRecordSize(student)
		}
	}

	return students, nil
}

func (vs *VariableStorage) AddStudents(filename string, students []entity.Student) error {
	existingStudents, err := vs.GetAllStudents(filename)
	if err != nil {
		return fmt.Errorf("erro ao ler alunos existentes: %w", err)
	}

	allStudents := make([]entity.Student, len(existingStudents))
	for i, s := range existingStudents {
		allStudents[i] = *s
	}

	allStudents = append(allStudents, students...)

	err = vs.WriteStudents(filename, allStudents)
	if err != nil {
		return err
	}

	vs.calculateFinalStats()
	return nil
}
