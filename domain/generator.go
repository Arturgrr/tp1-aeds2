package domain

import (
	"aeds2-tp1/entity"
	"fmt"
	"math/rand"
	"time"
)

type StudentGenerator struct {
	random *rand.Rand
}

func NewStudentGenerator() *StudentGenerator {
	return &StudentGenerator{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sg *StudentGenerator) Generate(count int) []entity.Student {
	students := make([]entity.Student, 0, count)
	
	nomes := []string{
		"João Silva", "Maria Santos", "Pedro Oliveira", "Ana Costa", "Carlos Ferreira",
		"Juliana Almeida", "Roberto Souza", "Fernanda Lima", "Lucas Rodrigues", "Patricia Gomes",
		"Marcos Pereira", "Camila Martins", "Rafael Barbosa", "Bruna Rocha", "Thiago Dias",
		"Larissa Araujo", "Felipe Ribeiro", "Gabriela Nascimento", "Gustavo Moura", "Isabela Teixeira",
	}
	
	cursos := []string{
		"Ciência da Computação", "Engenharia de Software", "Sistemas de Informação",
		"Engenharia Civil", "Administração", "Direito", "Medicina", "Psicologia",
		"Economia", "Arquitetura",
	}
	
	nomesMae := []string{
		"Maria Silva", "Ana Santos", "Carmen Oliveira", "Lucia Costa", "Rosa Ferreira",
		"Tereza Almeida", "Julia Souza", "Helena Lima", "Cecilia Rodrigues", "Marta Gomes",
	}
	
	nomesPai := []string{
		"José Silva", "Antonio Santos", "Paulo Oliveira", "Carlos Costa", "Francisco Ferreira",
		"Roberto Almeida", "João Souza", "Marcos Lima", "Ricardo Rodrigues", "Eduardo Gomes",
	}
	
	generated := 0
	for generated < count {
		matricula := 100000000 + generated + 1
		cpf := sg.generateCPF()
		anoIngresso := 2015 + sg.random.Intn(10)
		ca := 5.0 + sg.random.Float64()*5.0
		ca = float64(int(ca*100)) / 100
		
		student := entity.Student{
			Matricula:   matricula,
			Nome:        nomes[sg.random.Intn(len(nomes))],
			CPF:         cpf,
			Curso:       cursos[sg.random.Intn(len(cursos))],
			FiliacaoMae: nomesMae[sg.random.Intn(len(nomesMae))],
			FiliacaoPai: nomesPai[sg.random.Intn(len(nomesPai))],
			AnoIngresso: anoIngresso,
			CA:          ca,
		}
		
		student.TruncateFields()
		if err := student.Validate(); err != nil {
			fmt.Printf("Aviso: estudante gerado inválido: %v\n", err)
			continue
		}
		students = append(students, student)
		generated++
	}
	
	return students
}

func (sg *StudentGenerator) generateCPF() string {
	cpf := ""
	for i := 0; i < 11; i++ {
		cpf += fmt.Sprintf("%d", sg.random.Intn(10))
	}
	return cpf
}
