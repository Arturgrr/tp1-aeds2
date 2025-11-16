package main

import (
	"aeds2-tp1/domain"
	"aeds2-tp1/entity"
	"aeds2-tp1/infrastructure"
	"aeds2-tp1/storage"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const filename = "alunos.dat"

func main() {
	fmt.Println("=== Sistema de Armazenamento de Registros de Alunos ===")
	fmt.Println()

	if _, err := os.Stat(filename); err == nil {
		err := os.Remove(filename)
		if err != nil {
			fmt.Printf("Aviso: não foi possível deletar o arquivo %s existente: %v\n", filename, err)
		} else {
			fmt.Printf("Arquivo %s existente foi deletado. Criando novo arquivo.\n", filename)
		}
	}

	reader := bufio.NewReader(os.Stdin)

	numRecords := readInt(reader, "Digite o número de registros a serem gerados: ")
	blockSize := readInt(reader, "Digite o tamanho máximo do bloco (em bytes): ")

	fmt.Println("\nModo de armazenamento:")
	fmt.Println("1 - Registros de tamanho fixo")
	fmt.Println("2 - Registros de tamanho variável")
	storageMode := readInt(reader, "Escolha o modo (1 ou 2): ")

	var storageImpl storage.Storage
	var err error

	if storageMode == 1 {
		storageImpl, err = storage.NewFixedStorage(blockSize)
		if err != nil {
			fmt.Printf("Erro: %v\n", err)
			return
		}
	} else if storageMode == 2 {
		fmt.Println("\nTipo de armazenamento variável:")
		fmt.Println("1 - Contíguo (sem espalhamento)")
		fmt.Println("2 - Espalhado (com fragmentação entre blocos)")
		fragmentedMode := readInt(reader, "Escolha o tipo (1 ou 2): ")

		if fragmentedMode == 1 {
			storageImpl, err = storage.NewVariableStorage(blockSize)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
				return
			}
		} else if fragmentedMode == 2 {
			storageImpl, err = storage.NewVariableFragmentedStorage(blockSize)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
				return
			}
		} else {
			fmt.Println("Tipo inválido, usando contíguo por padrão")
			storageImpl, err = storage.NewVariableStorage(blockSize)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
				return
			}
		}
	} else {
		fmt.Println("Modo inválido, usando tamanho variável contíguo por padrão")
		storageImpl, err = storage.NewVariableStorage(blockSize)
		if err != nil {
			fmt.Printf("Erro: %v\n", err)
			return
		}
	}

	fmt.Println("\nGerando registros de alunos...")
	generator := domain.NewStudentGenerator()
	students := generator.Generate(numRecords)
	fmt.Printf("Gerados %d registros de alunos\n", len(students))

	fmt.Println("\nGravando registros no arquivo alunos.dat...")
	err = storageImpl.WriteStudents(filename, students)
	if err != nil {
		fmt.Printf("Erro ao gravar arquivo: %v\n", err)
		return
	}
	fmt.Println("Arquivo gravado com sucesso!")

	stats := storageImpl.GetStats(filename)
	reporter := infrastructure.NewReporter(stats)
	reporter.PrintStats()
	reporter.PrintBlockMap()
	reporter.PrintBlockVisualization()

	runQueryMode(reader, storageImpl)
}

func runQueryMode(reader *bufio.Reader, storageImpl storage.Storage) {
	for {
		fmt.Println("\n=== MENU PRINCIPAL ===")
		fmt.Println("1 - Consultar aluno por matrícula")
		fmt.Println("2 - Consultar todos os alunos")
		fmt.Println("3 - Registrar novos alunos")
		fmt.Println("4 - Ver relatório de armazenamento")
		fmt.Println("5 - Sair")
		option := readInt(reader, "Escolha uma opção: ")

		switch option {
		case 1:
			matricula := readInt(reader, "Digite a matrícula do aluno: ")
			student, err := storageImpl.FindStudentByMatricula(filename, matricula)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
			} else {
				printStudent(student)
			}
		case 2:
			listAllStudents(storageImpl)
		case 3:
			registerNewStudents(reader, storageImpl)
		case 4:
			showStorageReport(storageImpl)
		case 5:
			return
		default:
			fmt.Println("Opção inválida!")
		}
	}
}

func listAllStudents(storageImpl storage.Storage) {
	fmt.Println("\n=== TODOS OS ALUNOS ===")
	students, err := storageImpl.GetAllStudents(filename)
	if err != nil {
		fmt.Printf("Erro ao listar alunos: %v\n", err)
		return
	}

	if len(students) == 0 {
		fmt.Println("Nenhum aluno encontrado.")
		return
	}

	fmt.Printf("Total de alunos: %d\n\n", len(students))
	for i, student := range students {
		fmt.Printf("%d. Matrícula: %d - %s - Curso: %s - CA: %.2f\n", 
			i+1, student.Matricula, student.Nome, student.Curso, student.CA)
	}
}

func registerNewStudents(reader *bufio.Reader, storageImpl storage.Storage) {
	fmt.Println("\n=== REGISTRAR NOVOS ALUNOS ===")
	numRecords := readInt(reader, "Digite o número de alunos a serem gerados: ")
	
	fmt.Println("\nGerando novos alunos...")
	generator := domain.NewStudentGenerator()
	students := generator.Generate(numRecords)
	fmt.Printf("Gerados %d novos alunos\n", len(students))

	fmt.Println("\nAdicionando alunos ao arquivo...")
	err := storageImpl.AddStudents(filename, students)
	if err != nil {
		fmt.Printf("Erro ao adicionar alunos: %v\n", err)
		return
	}
	fmt.Println("Alunos adicionados com sucesso!")
}

func showStorageReport(storageImpl storage.Storage) {
	stats := storageImpl.GetStats(filename)
	reporter := infrastructure.NewReporter(stats)
	reporter.PrintStats()
	reporter.PrintBlockMap()
	reporter.PrintBlockVisualization()
}


func printStudent(student *entity.Student) {
	fmt.Println("\n=== DADOS DO ALUNO ===")
	fmt.Printf("Matrícula:     %d\n", student.Matricula)
	fmt.Printf("Nome:           %s\n", student.Nome)
	fmt.Printf("CPF:            %s\n", student.CPF)
	fmt.Printf("Curso:          %s\n", student.Curso)
	fmt.Printf("Filiação Mãe:   %s\n", student.FiliacaoMae)
	fmt.Printf("Filiação Pai:   %s\n", student.FiliacaoPai)
	fmt.Printf("Ano de Ingresso: %d\n", student.AnoIngresso)
	fmt.Printf("CA:             %.2f\n", student.CA)
}

func readInt(reader *bufio.Reader, prompt string) int {
	for {
		fmt.Print(prompt)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		value, err := strconv.Atoi(input)
		if err == nil && value > 0 {
			return value
		}
		fmt.Println("Valor inválido. Digite um número inteiro positivo.")
	}
}

