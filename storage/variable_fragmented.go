package storage

import (
	"aeds2-tp1/entity"
	"encoding/binary"
	"fmt"
	"os"
)

type VariableFragmentedStorage struct {
	blockSize int
	stats     StorageStats
}

func NewVariableFragmentedStorage(blockSize int) (*VariableFragmentedStorage, error) {
	vfs := &VariableFragmentedStorage{
		blockSize: blockSize,
		stats: StorageStats{
			BlockStatsList: make([]BlockStats, 0),
		},
	}

	if err := vfs.ValidateBlockSize(blockSize); err != nil {
		return nil, err
	}

	return vfs, nil
}

func (vfs *VariableFragmentedStorage) ValidateBlockSize(blockSize int) error {
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

func (vfs *VariableFragmentedStorage) WriteStudents(filename string, students []entity.Student) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo: %w", err)
	}
	defer file.Close()

	currentBlock := make([]byte, 0, vfs.blockSize)
	currentBlockNumber := 0
	blockStats := BlockStats{
		BlockNumber: currentBlockNumber,
		BytesUsed:   0,
		BytesTotal:  vfs.blockSize,
	}

	for i, student := range students {
		recordData := vfs.serializeStudent(student)
		vfs.writeFragmentedRecord(&currentBlock, &currentBlockNumber, &blockStats, recordData, file, i == len(students)-1)
	}

	if len(currentBlock) > 0 {
		blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
		vfs.stats.BlockStatsList = append(vfs.stats.BlockStatsList, blockStats)
		vfs.writeBlock(file, currentBlock)
		vfs.stats.TotalBlocks++
		vfs.stats.TotalBytesUsed += blockStats.BytesUsed
		vfs.stats.TotalBytesTotal += blockStats.BytesTotal
	}

	vfs.calculateFinalStats()
	return nil
}

func (vfs *VariableFragmentedStorage) writeFragmentedRecord(currentBlock *[]byte, currentBlockNumber *int, blockStats *BlockStats, recordData []byte, file *os.File, isLast bool) {
	recordSize := len(recordData)
	availableSpace := vfs.blockSize - len(*currentBlock)

	if recordSize <= availableSpace {
		*currentBlock = append(*currentBlock, recordData...)
		blockStats.BytesUsed += recordSize
		blockStats.RecordsCount++
	} else {
		if len(*currentBlock) > 0 {
			blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
			if blockStats.OccupancyRate < 100 {
				vfs.stats.PartialBlocks++
			}
			vfs.stats.BlockStatsList = append(vfs.stats.BlockStatsList, *blockStats)
			vfs.writeBlock(file, *currentBlock)
			vfs.stats.TotalBlocks++
			vfs.stats.TotalBytesUsed += blockStats.BytesUsed
			vfs.stats.TotalBytesTotal += blockStats.BytesTotal
		}

		remainingData := recordData
		headerSize := 5
		isFirstChunk := true

		for len(remainingData) > 0 {
			*currentBlock = make([]byte, 0, vfs.blockSize)
			*currentBlockNumber++
			*blockStats = BlockStats{
				BlockNumber: *currentBlockNumber,
				BytesUsed:   0,
				BytesTotal:  vfs.blockSize,
			}

			spaceAvailable := vfs.blockSize - headerSize
			chunkSize := len(remainingData)
			if chunkSize > spaceAvailable {
				chunkSize = spaceAvailable
			}

			continuationFlag := byte(1)
			if len(remainingData) <= spaceAvailable {
				continuationFlag = byte(0)
			}

			*currentBlock = append(*currentBlock, continuationFlag)

			sizeBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(sizeBytes, uint32(chunkSize))
			*currentBlock = append(*currentBlock, sizeBytes...)

			*currentBlock = append(*currentBlock, remainingData[:chunkSize]...)
			blockStats.BytesUsed += headerSize + chunkSize
			if isFirstChunk {
				blockStats.RecordsCount++
				isFirstChunk = false
			}

			remainingData = remainingData[chunkSize:]

			blockStats.OccupancyRate = float64(blockStats.BytesUsed) / float64(blockStats.BytesTotal) * 100
			if blockStats.OccupancyRate < 100 {
				vfs.stats.PartialBlocks++
			}
			vfs.stats.BlockStatsList = append(vfs.stats.BlockStatsList, *blockStats)
			vfs.writeBlock(file, *currentBlock)
			vfs.stats.TotalBlocks++
			vfs.stats.TotalBytesUsed += blockStats.BytesUsed
			vfs.stats.TotalBytesTotal += blockStats.BytesTotal
		}
	}
}

func (vfs *VariableFragmentedStorage) serializeStudent(student entity.Student) []byte {
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

func (vfs *VariableFragmentedStorage) writeBlock(file *os.File, block []byte) {
	paddedBlock := make([]byte, vfs.blockSize)
	copy(paddedBlock, block)
	file.Write(paddedBlock)
}

func (vfs *VariableFragmentedStorage) calculateFinalStats() {
	if vfs.stats.TotalBytesTotal > 0 {
		vfs.stats.EfficiencyRate = float64(vfs.stats.TotalBytesUsed) / float64(vfs.stats.TotalBytesTotal) * 100
	}
}

func (vfs *VariableFragmentedStorage) GetStats(filename string) StorageStats {
	vfs.recalculateStatsFromFile(filename)
	return vfs.stats
}

func (vfs *VariableFragmentedStorage) recalculateStatsFromFile(filename string) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return
	}

	totalBlocks := int(fileInfo.Size()) / vfs.blockSize
	vfs.stats = StorageStats{
		TotalBlocks:     totalBlocks,
		TotalBytesTotal: totalBlocks * vfs.blockSize,
		BlockStatsList:  make([]BlockStats, 0),
	}

	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	totalUsed := 0
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vfs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vfs.blockSize))
		if err != nil {
			continue
		}

		bytesUsed := 0
		recordsCount := 0
		offset := 0
		for offset < vfs.blockSize {
			if offset+5 > vfs.blockSize {
				break
			}

			continuationFlag := block[offset]
			chunkSizeBytes := block[offset+1 : offset+5]
			chunkSize := int(binary.LittleEndian.Uint32(chunkSizeBytes))

			if offset+5+chunkSize > vfs.blockSize {
				break
			}

			bytesUsed += 5 + chunkSize
			if continuationFlag == 0 {
				recordsCount++
			}

			if continuationFlag == 0 {
				offset += 5 + chunkSize
			} else {
				offset = vfs.blockSize
			}
		}

		totalUsed += bytesUsed

		occupancyRate := float64(bytesUsed) / float64(vfs.blockSize) * 100
		blockStats := BlockStats{
			BlockNumber:   blockNum,
			BytesUsed:     bytesUsed,
			BytesTotal:    vfs.blockSize,
			OccupancyRate: occupancyRate,
			RecordsCount:  recordsCount,
		}

		if occupancyRate < 100 && occupancyRate > 0 {
			vfs.stats.PartialBlocks++
		}

		vfs.stats.BlockStatsList = append(vfs.stats.BlockStatsList, blockStats)
	}

	vfs.stats.TotalBytesUsed = totalUsed
	if vfs.stats.TotalBytesTotal > 0 {
		vfs.stats.EfficiencyRate = float64(vfs.stats.TotalBytesUsed) / float64(vfs.stats.TotalBytesTotal) * 100
	}
}

func (vfs *VariableFragmentedStorage) GetBlockSize() int {
	return vfs.blockSize
}

func (vfs *VariableFragmentedStorage) FindStudentByMatricula(filename string, matricula int) (*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / vfs.blockSize
	return vfs.findStudentFragmented(file, totalBlocks, matricula)
}

func (vfs *VariableFragmentedStorage) findStudentFragmented(file *os.File, totalBlocks int, matricula int) (*entity.Student, error) {
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vfs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vfs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset < vfs.blockSize {
			if offset+5 > vfs.blockSize {
				break
			}

			continuationFlag := block[offset]
			chunkSizeBytes := block[offset+1 : offset+5]
			chunkSize := int(binary.LittleEndian.Uint32(chunkSizeBytes))

			if offset+5+chunkSize > vfs.blockSize {
				break
			}

			chunkData := make([]byte, chunkSize)
			copy(chunkData, block[offset+5:offset+5+chunkSize])

			recordData := chunkData
			currentBlockNum := blockNum
			currentContFlag := continuationFlag

			for currentContFlag == 1 {
				currentBlockNum++
				if currentBlockNum >= totalBlocks {
					break
				}

				nextBlock := make([]byte, vfs.blockSize)
				_, err := file.ReadAt(nextBlock, int64(currentBlockNum*vfs.blockSize))
				if err != nil {
					break
				}

				nextContFlag := nextBlock[0]
				nextChunkSizeBytes := nextBlock[1:5]
				nextChunkSize := int(binary.LittleEndian.Uint32(nextChunkSizeBytes))

				if 5+nextChunkSize <= vfs.blockSize {
					recordData = append(recordData, nextBlock[5:5+nextChunkSize]...)
				}

				currentContFlag = nextContFlag
			}

			if len(recordData) >= 4 {
				readMatricula := int(binary.LittleEndian.Uint32(recordData[:4]))
				if readMatricula == matricula {
					student, err := vfs.deserializeStudent(recordData)
					if err == nil {
						return student, nil
					}
				}
			}

			if continuationFlag == 0 {
				offset += 5 + chunkSize
			} else {
				offset = vfs.blockSize
			}
		}
	}

	return nil, fmt.Errorf("aluno com matrícula %d não encontrado", matricula)
}

func (vfs *VariableFragmentedStorage) deserializeStudent(data []byte) (*entity.Student, error) {
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

func (vfs *VariableFragmentedStorage) GetAllStudents(filename string) ([]*entity.Student, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter informações do arquivo: %w", err)
	}

	totalBlocks := int(fileInfo.Size()) / vfs.blockSize
	students := make([]*entity.Student, 0)

	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		block := make([]byte, vfs.blockSize)
		_, err := file.ReadAt(block, int64(blockNum*vfs.blockSize))
		if err != nil {
			continue
		}

		offset := 0
		for offset < vfs.blockSize {
			if offset+5 > vfs.blockSize {
				break
			}

			continuationFlag := block[offset]
			chunkSizeBytes := block[offset+1 : offset+5]
			chunkSize := int(binary.LittleEndian.Uint32(chunkSizeBytes))

			if offset+5+chunkSize > vfs.blockSize {
				break
			}

			chunkData := make([]byte, chunkSize)
			copy(chunkData, block[offset+5:offset+5+chunkSize])

			recordData := chunkData
			currentBlockNum := blockNum
			currentContFlag := continuationFlag

			for currentContFlag == 1 {
				currentBlockNum++
				if currentBlockNum >= totalBlocks {
					break
				}

				nextBlock := make([]byte, vfs.blockSize)
				_, err := file.ReadAt(nextBlock, int64(currentBlockNum*vfs.blockSize))
				if err != nil {
					break
				}

				nextContFlag := nextBlock[0]
				nextChunkSizeBytes := nextBlock[1:5]
				nextChunkSize := int(binary.LittleEndian.Uint32(nextChunkSizeBytes))

				if 5+nextChunkSize <= vfs.blockSize {
					recordData = append(recordData, nextBlock[5:5+nextChunkSize]...)
				}

				currentContFlag = nextContFlag
			}

			if len(recordData) >= 4 {
				student, err := vfs.deserializeStudent(recordData)
				if err == nil && student.Matricula > 0 {
					students = append(students, student)
				}
			}

			if continuationFlag == 0 {
				offset += 5 + chunkSize
			} else {
				offset = vfs.blockSize
			}
		}
	}

	return students, nil
}

func (vfs *VariableFragmentedStorage) AddStudents(filename string, students []entity.Student) error {
	existingStudents, err := vfs.GetAllStudents(filename)
	if err != nil {
		return fmt.Errorf("erro ao ler alunos existentes: %w", err)
	}

	allStudents := make([]entity.Student, len(existingStudents))
	for i, s := range existingStudents {
		allStudents[i] = *s
	}

	allStudents = append(allStudents, students...)

	err = vfs.WriteStudents(filename, allStudents)
	if err != nil {
		return err
	}

	vfs.calculateFinalStats()
	return nil
}
