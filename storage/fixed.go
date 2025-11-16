package storage

import (
	"aeds2-tp1/entity"
	"encoding/binary"
	"fmt"
	"os"
)

type FixedStorage struct {
	blockSize       int
	fixedRecordSize int
	stats           StorageStats
}

func NewFixedStorage(blockSize int) (*FixedStorage, error) {
	fs := &FixedStorage{
		blockSize: blockSize,
		stats: StorageStats{
			BlockStatsList: make([]BlockStats, 0),
		},
	}
	
	if err := fs.ValidateBlockSize(blockSize); err != nil {
		return nil, err
	}
	
	return fs, nil
}

func (fs *FixedStorage) ValidateBlockSize(blockSize int) error {
	minSize := 4 +
		entity.MaxNomeLength +
		entity.CPFLength +
		entity.MaxCursoLength +
		entity.MaxFiliacaoLength +
		entity.MaxFiliacaoLength +
		4 +
		8
	
	if blockSize < minSize {
		return fmt.Errorf("tamanho do bloco (%d bytes) é menor que o tamanho mínimo necessário para um registro (%d bytes)", blockSize, minSize)
	}
	
	return nil
}

func (fs *FixedStorage) calculateFixedRecordSize() {
	fs.fixedRecordSize = 4 +
		entity.MaxNomeLength +
		entity.CPFLength +
		entity.MaxCursoLength +
		entity.MaxFiliacaoLength +
		entity.MaxFiliacaoLength +
		4 +
		8
}

func (fs *FixedStorage) WriteStudents(filename string, students []entity.Student) error {
	fs.calculateFixedRecordSize()
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo: %w", err)
	}
	defer file.Close()

	currentBlock := make([]byte, 0, fs.blockSize)
	currentBlockNumber := 0
	blockStats := BlockStats{
		BlockNumber: currentBlockNumber,
		BytesUsed:   0,
		BytesTotal:  fs.blockSize,
	}

	for i, student := range students {
		recordData := fs.serializeStudentFixed(student)
		fs.writeContiguousRecord(&currentBlock, &currentBlockNumber, &blockStats, recordData, file, i == len(students)-1)
	}

	if len(currentBlock) > 0 {
		blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
		fs.stats.BlockStatsList = append(fs.stats.BlockStatsList, blockStats)
		fs.writeBlock(file, currentBlock)
		fs.stats.TotalBlocks++
		fs.stats.TotalBytesUsed += blockStats.BytesUsed
		fs.stats.TotalBytesTotal += blockStats.BytesTotal
	}

	fs.calculateFinalStats()
	return nil
}

func (fs *FixedStorage) serializeStudentFixed(student entity.Student) []byte {
	data := make([]byte, 0, fs.fixedRecordSize)
	
	matriculaBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(matriculaBytes, uint32(student.Matricula))
	data = append(data, matriculaBytes...)
	
	nomeBytes := []byte(student.Nome)
	maxNomeLen := entity.MaxNomeLength
	if len(nomeBytes) > maxNomeLen {
		nomeBytes = nomeBytes[:maxNomeLen]
	}
	paddedNome := make([]byte, maxNomeLen)
	copy(paddedNome, nomeBytes)
	for i := len(nomeBytes); i < maxNomeLen; i++ {
		paddedNome[i] = '#'
	}
	data = append(data, paddedNome...)
	
	cpfBytes := []byte(student.CPF)
	if len(cpfBytes) > entity.CPFLength {
		cpfBytes = cpfBytes[:entity.CPFLength]
	}
	paddedCPF := make([]byte, entity.CPFLength)
	copy(paddedCPF, cpfBytes)
	for i := len(cpfBytes); i < entity.CPFLength; i++ {
		paddedCPF[i] = '0'
	}
	data = append(data, paddedCPF...)
	
	cursoBytes := []byte(student.Curso)
	maxCursoLen := entity.MaxCursoLength
	if len(cursoBytes) > maxCursoLen {
		cursoBytes = cursoBytes[:maxCursoLen]
	}
	paddedCurso := make([]byte, maxCursoLen)
	copy(paddedCurso, cursoBytes)
	for i := len(cursoBytes); i < maxCursoLen; i++ {
		paddedCurso[i] = '#'
	}
	data = append(data, paddedCurso...)
	
	maeBytes := []byte(student.FiliacaoMae)
	maxMaeLen := entity.MaxFiliacaoLength
	if len(maeBytes) > maxMaeLen {
		maeBytes = maeBytes[:maxMaeLen]
	}
	paddedMae := make([]byte, maxMaeLen)
	copy(paddedMae, maeBytes)
	for i := len(maeBytes); i < maxMaeLen; i++ {
		paddedMae[i] = '#'
	}
	data = append(data, paddedMae...)
	
	paiBytes := []byte(student.FiliacaoPai)
	maxPaiLen := entity.MaxFiliacaoLength
	if len(paiBytes) > maxPaiLen {
		paiBytes = paiBytes[:maxPaiLen]
	}
	paddedPai := make([]byte, maxPaiLen)
	copy(paddedPai, paiBytes)
	for i := len(paiBytes); i < maxPaiLen; i++ {
		paddedPai[i] = '#'
	}
	data = append(data, paddedPai...)
	
	anoBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(anoBytes, uint32(student.AnoIngresso))
	data = append(data, anoBytes...)
	
	caBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(caBytes, uint64(student.CA*100))
	data = append(data, caBytes...)
	
	return data
}

func (fs *FixedStorage) writeContiguousRecord(currentBlock *[]byte, currentBlockNumber *int, blockStats *BlockStats, recordData []byte, file *os.File, isLast bool) {
	recordSize := len(recordData)
	
	if len(*currentBlock)+recordSize > fs.blockSize {
		if len(*currentBlock) > 0 {
			blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
			if blockStats.OccupancyRate < 100 {
				fs.stats.PartialBlocks++
			}
			fs.stats.BlockStatsList = append(fs.stats.BlockStatsList, *blockStats)
			fs.writeBlock(file, *currentBlock)
			fs.stats.TotalBlocks++
			fs.stats.TotalBytesUsed += blockStats.BytesUsed
			fs.stats.TotalBytesTotal += blockStats.BytesTotal
		}
		
		*currentBlock = make([]byte, 0, fs.blockSize)
		*currentBlockNumber++
		*blockStats = BlockStats{
			BlockNumber: *currentBlockNumber,
			BytesUsed:   0,
			BytesTotal:  fs.blockSize,
		}
	}
	
	*currentBlock = append(*currentBlock, recordData...)
	blockStats.BytesUsed += recordSize
	blockStats.RecordsCount++
}

func (fs *FixedStorage) writeBlock(file *os.File, block []byte) {
	paddedBlock := make([]byte, fs.blockSize)
	copy(paddedBlock, block)
	file.Write(paddedBlock)
}

func (fs *FixedStorage) calculateFinalStats() {
	if fs.stats.TotalBytesTotal > 0 {
		fs.stats.EfficiencyRate = float64(fs.stats.TotalBytesUsed) / float64(fs.stats.TotalBytesTotal) * 100
	}
}

func (fs *FixedStorage) GetStats(filename string) StorageStats {
	fs.recalculateStatsFromFile(filename)
	return fs.stats
}

func (fs *FixedStorage) recalculateStatsFromFile(filename string) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return
	}

	totalBlocks := int(fileInfo.Size()) / fs.blockSize
	fs.stats = StorageStats{
		TotalBlocks:     totalBlocks,
		TotalBytesTotal: totalBlocks * fs.blockSize,
		BlockStatsList:  make([]BlockStats, 0),
	}

	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	totalUsed := 0
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, fs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*fs.blockSize))
		if err != nil {
			continue
		}

		recordsCount := 0
		offset := 0
		for offset+fs.fixedRecordSize <= fs.blockSize {
			if offset+4 > fs.blockSize {
				break
			}

			matriculaBytes := block[offset : offset+4]
			matricula := int(binary.LittleEndian.Uint32(matriculaBytes))
			
			if matricula > 0 {
				recordsCount++
			}

			offset += fs.fixedRecordSize
		}

		bytesUsed := recordsCount * fs.fixedRecordSize
		totalUsed += bytesUsed

		occupancyRate := float64(bytesUsed) / float64(fs.blockSize) * 100
		blockStats := BlockStats{
			BlockNumber:   blockNum,
			BytesUsed:     bytesUsed,
			BytesTotal:    fs.blockSize,
			OccupancyRate: occupancyRate,
			RecordsCount:  recordsCount,
		}

		if occupancyRate < 100 && occupancyRate > 0 {
			fs.stats.PartialBlocks++
		}

		fs.stats.BlockStatsList = append(fs.stats.BlockStatsList, blockStats)
	}

	fs.stats.TotalBytesUsed = totalUsed
	if fs.stats.TotalBytesTotal > 0 {
		fs.stats.EfficiencyRate = float64(fs.stats.TotalBytesUsed) / float64(fs.stats.TotalBytesTotal) * 100
	}
}

func (fs *FixedStorage) FindStudentByMatricula(filename string, matricula int) (*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / fs.blockSize
	return fs.findStudentFixed(file, totalBlocks, matricula)
}

func (fs *FixedStorage) findStudentFixed(file *os.File, totalBlocks int, matricula int) (*entity.Student, error) {
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, fs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*fs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset+fs.fixedRecordSize <= fs.blockSize {
			if offset+4 > fs.blockSize {
				break
			}

			matriculaBytes := block[offset : offset+4]
			readMatricula := int(binary.LittleEndian.Uint32(matriculaBytes))
			
			if readMatricula == matricula {
				student, err := fs.deserializeStudentFixed(block[offset:offset+fs.fixedRecordSize])
				if err == nil {
					return student, nil
				}
			}

			offset += fs.fixedRecordSize
		}
	}

	return nil, fmt.Errorf("aluno com matrícula %d não encontrado", matricula)
}

func (fs *FixedStorage) deserializeStudentFixed(data []byte) (*entity.Student, error) {
	if len(data) < fs.fixedRecordSize {
		return nil, fmt.Errorf("dados insuficientes")
	}

	offset := 0

	matricula := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	nomeBytes := data[offset : offset+entity.MaxNomeLength]
	nome := string(nomeBytes)
	for len(nome) > 0 && nome[len(nome)-1] == '#' {
		nome = nome[:len(nome)-1]
	}
	offset += entity.MaxNomeLength

	cpfBytes := data[offset : offset+entity.CPFLength]
	cpf := string(cpfBytes)
	offset += entity.CPFLength

	cursoBytes := data[offset : offset+entity.MaxCursoLength]
	curso := string(cursoBytes)
	for len(curso) > 0 && curso[len(curso)-1] == '#' {
		curso = curso[:len(curso)-1]
	}
	offset += entity.MaxCursoLength

	maeBytes := data[offset : offset+entity.MaxFiliacaoLength]
	filiacaoMae := string(maeBytes)
	for len(filiacaoMae) > 0 && filiacaoMae[len(filiacaoMae)-1] == '#' {
		filiacaoMae = filiacaoMae[:len(filiacaoMae)-1]
	}
	offset += entity.MaxFiliacaoLength

	paiBytes := data[offset : offset+entity.MaxFiliacaoLength]
	filiacaoPai := string(paiBytes)
	for len(filiacaoPai) > 0 && filiacaoPai[len(filiacaoPai)-1] == '#' {
		filiacaoPai = filiacaoPai[:len(filiacaoPai)-1]
	}
	offset += entity.MaxFiliacaoLength

	anoIngresso := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

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

	if err := student.Validate(); err != nil {
		return nil, fmt.Errorf("estudante deserializado inválido: %w", err)
	}

	return student, nil
}

func (fs *FixedStorage) GetBlockSize() int {
	return fs.blockSize
}

func (fs *FixedStorage) GetAllStudents(filename string) ([]*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / fs.blockSize
	students := make([]*entity.Student, 0)

	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, fs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*fs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset+fs.fixedRecordSize <= fs.blockSize {
			if offset+4 > fs.blockSize {
				break
			}

			student, err := fs.deserializeStudentFixed(block[offset:offset+fs.fixedRecordSize])
			if err == nil && student.Matricula > 0 {
				students = append(students, student)
			}

			offset += fs.fixedRecordSize
		}
	}

	return students, nil
}

func (fs *FixedStorage) AddStudents(filename string, students []entity.Student) error {
	existingStudents, err := fs.GetAllStudents(filename)
	if err != nil {
		return fmt.Errorf("erro ao ler alunos existentes: %w", err)
	}

	allStudents := make([]entity.Student, len(existingStudents))
	for i, s := range existingStudents {
		allStudents[i] = *s
	}

	allStudents = append(allStudents, students...)

	err = fs.WriteStudents(filename, allStudents)
	if err != nil {
		return err
	}

	fs.calculateFinalStats()
	return nil
}
