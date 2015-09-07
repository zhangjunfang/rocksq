package rocksq

import "encoding/binary"

type _CountMerger struct{}

func (cm *_CountMerger) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
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

func (cm *_CountMerger) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	leftValue := cm.uintValue(leftOperand)
	rightValue := cm.uintValue(rightOperand)
	return cm.uintData(leftValue + rightValue), true
}

func (cm *_CountMerger) Name() string {
	return "rocksq.count_merger"
}

func (cm *_CountMerger) uintValue(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func (cm *_CountMerger) uintData(i uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], i)
	return k[:]
}
