package rocksq

import "encoding/binary"

type CountMerger struct{}

func (cm *CountMerger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var curValue uint64
	if len(existingValue) > 0 {
		curValue = cm.uintValue(existingValue)
	}
	for _, operand := range operands {
		value := cm.uintValue(operand)
		curValue += value
	}
	return cm.uintData(curValue), true
}

func (cm *CountMerger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	leftValue := cm.uintValue(leftOperand)
	rightValue := cm.uintValue(rightOperand)
	return cm.uintData(leftValue + rightValue), true
}

func (cm *CountMerger) Name() string {
	return "rocksq.count_merger"
}

func (cm *CountMerger) uintValue(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func (cm *CountMerger) uintData(i uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], i)
	return k[:]
}
