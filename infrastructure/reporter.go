package infrastructure

import (
	"aeds2-tp1/storage"
	"fmt"
)

type Reporter struct {
	stats storage.StorageStats
}

func NewReporter(stats storage.StorageStats) *Reporter {
	return &Reporter{
		stats: stats,
	}
}

func (r *Reporter) PrintStats() {
	fmt.Println("\n=== ESTATÍSTICAS DE ARMAZENAMENTO ===")
	fmt.Printf("Número total de blocos utilizados: %d\n", r.stats.TotalBlocks)
	fmt.Printf("Eficiência total de armazenamento: %.2f%%\n", r.stats.EfficiencyRate)
	fmt.Printf("Número de blocos parcialmente utilizados: %d\n", r.stats.PartialBlocks)
	fmt.Printf("Total de bytes utilizados: %d\n", r.stats.TotalBytesUsed)
	fmt.Printf("Total de bytes disponíveis: %d\n", r.stats.TotalBytesTotal)
	
	avgOccupancy := 0.0
	for _, blockStat := range r.stats.BlockStatsList {
		avgOccupancy += blockStat.OccupancyRate
	}
	if len(r.stats.BlockStatsList) > 0 {
		avgOccupancy /= float64(len(r.stats.BlockStatsList))
	}
	fmt.Printf("Percentual médio de ocupação: %.2f%%\n", avgOccupancy)
}

func (r *Reporter) PrintBlockMap() {
	fmt.Println("\n=== MAPA DE OCUPAÇÃO DOS BLOCOS ===")
	for _, blockStat := range r.stats.BlockStatsList {
		fmt.Printf("Bloco %d: %d bytes (%.2f%% cheio) - %d registros\n",
			blockStat.BlockNumber+1,
			blockStat.BytesUsed,
			blockStat.OccupancyRate,
			blockStat.RecordsCount)
	}
}

func (r *Reporter) PrintBlockVisualization() {
	fmt.Println("\n=== VISUALIZAÇÃO VISUAL DOS BLOCOS ===")
	for _, blockStat := range r.stats.BlockStatsList {
		barLength := int(blockStat.OccupancyRate / 2)
		bar := ""
		for i := 0; i < barLength; i++ {
			bar += "█"
		}
		for i := barLength; i < 50; i++ {
			bar += "░"
		}
		fmt.Printf("Bloco %3d: [%s] %.2f%%\n", blockStat.BlockNumber+1, bar, blockStat.OccupancyRate)
	}
}
