package entity

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/go-playground/validator/v10"
)

const (
	MaxMatriculaDigits = 9
	MaxNomeLength      = 50
	CPFLength          = 11
	MaxCursoLength     = 30
	MaxFiliacaoLength  = 30
	AnoIngressoMin     = 1000
	AnoIngressoMax     = 9999
	CA_MIN             = 0.0
	CA_MAX             = 10.0
)

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterValidation("matricula_digits", validateMatriculaDigits)
	validate.RegisterValidation("cpf_format", validateCPFFormat)
}

func validateMatriculaDigits(fl validator.FieldLevel) bool {
	matricula := fl.Field().Int()
	matriculaStr := strconv.FormatInt(matricula, 10)
	return len(matriculaStr) <= MaxMatriculaDigits && matricula > 0
}

func validateCPFFormat(fl validator.FieldLevel) bool {
	cpf := fl.Field().String()
	if len(cpf) != CPFLength {
		return false
	}
	for _, char := range cpf {
		if !unicode.IsDigit(char) {
			return false
		}
	}
	return true
}

type Student struct {
	Matricula   int     `validate:"required,min=1,matricula_digits"`
	Nome        string  `validate:"required,min=1,max=50"`
	CPF         string  `validate:"required,cpf_format"`
	Curso       string  `validate:"required,min=1,max=30"`
	FiliacaoMae string  `validate:"required,min=1,max=30"`
	FiliacaoPai string  `validate:"required,min=1,max=30"`
	AnoIngresso int     `validate:"required,min=1000,max=9999"`
	CA          float64 `validate:"required,min=0.0,max=10.0"`
}

func (s *Student) Validate() error {
	if s.Matricula < 1 || len(strconv.Itoa(s.Matricula)) > MaxMatriculaDigits {
		return fmt.Errorf("matrícula deve ter no máximo %d dígitos", MaxMatriculaDigits)
	}

	if len(s.Nome) == 0 || len(s.Nome) > MaxNomeLength {
		return fmt.Errorf("nome deve ter entre 1 e %d caracteres", MaxNomeLength)
	}

	if len(s.CPF) != CPFLength {
		return fmt.Errorf("CPF deve ter exatamente %d caracteres", CPFLength)
	}

	for _, char := range s.CPF {
		if char < '0' || char > '9' {
			return errors.New("CPF deve conter apenas dígitos")
		}
	}

	if len(s.Curso) == 0 || len(s.Curso) > MaxCursoLength {
		return fmt.Errorf("curso deve ter entre 1 e %d caracteres", MaxCursoLength)
	}

	if len(s.FiliacaoMae) == 0 || len(s.FiliacaoMae) > MaxFiliacaoLength {
		return fmt.Errorf("filiação mãe deve ter entre 1 e %d caracteres", MaxFiliacaoLength)
	}

	if len(s.FiliacaoPai) == 0 || len(s.FiliacaoPai) > MaxFiliacaoLength {
		return fmt.Errorf("filiação pai deve ter entre 1 e %d caracteres", MaxFiliacaoLength)
	}

	if s.AnoIngresso < AnoIngressoMin || s.AnoIngresso > AnoIngressoMax {
		return fmt.Errorf("ano de ingresso deve ter %d dígitos", 4)
	}

	if s.CA < CA_MIN || s.CA > CA_MAX {
		return fmt.Errorf("CA deve estar entre %.2f e %.2f", CA_MIN, CA_MAX)
	}

	return nil
}

func (s *Student) TruncateFields() {
	if len(s.Nome) > MaxNomeLength {
		s.Nome = s.Nome[:MaxNomeLength]
	}

	if len(s.CPF) > CPFLength {
		s.CPF = s.CPF[:CPFLength]
	} else if len(s.CPF) < CPFLength {
		s.CPF = strings.Repeat("0", CPFLength-len(s.CPF)) + s.CPF
	}

	if len(s.Curso) > MaxCursoLength {
		s.Curso = s.Curso[:MaxCursoLength]
	}

	if len(s.FiliacaoMae) > MaxFiliacaoLength {
		s.FiliacaoMae = s.FiliacaoMae[:MaxFiliacaoLength]
	}

	if len(s.FiliacaoPai) > MaxFiliacaoLength {
		s.FiliacaoPai = s.FiliacaoPai[:MaxFiliacaoLength]
	}

	matriculaStr := strconv.Itoa(s.Matricula)
	if len(matriculaStr) > MaxMatriculaDigits {
		matriculaStr = matriculaStr[:MaxMatriculaDigits]
		s.Matricula, _ = strconv.Atoi(matriculaStr)
	}

	s.CA = math.Round(s.CA*100) / 100
	if s.CA < CA_MIN {
		s.CA = CA_MIN
	}
	if s.CA > CA_MAX {
		s.CA = CA_MAX
	}
}