package emulated

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/jackc/pgx/v5"
)

type TransactionRow struct {
	Account                  string
	Hash                     string
	Lt                       uint64
	BlockWorkchain           *int32
	BlockShard               *uint64
	BlockSeqno               *uint32
	McBlockSeqno             *uint32
	TraceID                  *string
	PrevTransHash            *string
	PrevTransLt              *uint64
	Now                      *uint32
	OrigStatus               *string
	EndStatus                *string
	TotalFees                *uint64
	TotalFeesExtraCurrencies map[string]string
	AccountStateHashBefore   *string
	AccountStateHashAfter    *string
	Descr                    *string
	Aborted                  *bool
	Destroyed                *bool
	CreditFirst              *bool
	IsTock                   *bool
	Installed                *bool
	StorageFeesCollected     *uint64
	StorageFeesDue           *uint64
	StorageStatusChange      *string
	CreditDueFeesCollected   *uint64
	Credit                   *uint64
	CreditExtraCurrencies    map[string]string
	ComputeSkipped           *bool
	SkippedReason            *string
	ComputeSuccess           *bool
	ComputeMsgStateUsed      *bool
	ComputeAccountActivated  *bool
	ComputeGasFees           *uint64
	ComputeGasUsed           *uint64
	ComputeGasLimit          *uint64
	ComputeGasCredit         *uint64
	ComputeMode              *int8
	ComputeExitCode          *int32
	ComputeExitArg           *int32
	ComputeVmSteps           *uint32
	ComputeVmInitStateHash   *string
	ComputeVmFinalStateHash  *string
	ActionSuccess            *bool
	ActionValid              *bool
	ActionNoFunds            *bool
	ActionStatusChange       *string
	ActionTotalFwdFees       *uint64
	ActionTotalActionFees    *uint64
	ActionResultCode         *int32
	ActionResultArg          *int32
	ActionTotActions         *uint16
	ActionSpecActions        *uint16
	ActionSkippedActions     *uint16
	ActionMsgsCreated        *uint16
	ActionActionListHash     *string
	ActionTotMsgSizeCells    *uint64
	ActionTotMsgSizeBits     *uint64
	Bounce                   *string
	BounceMsgSizeCells       *uint64
	BounceMsgSizeBits        *uint64
	BounceReqFwdFees         *uint64
	BounceMsgFees            *uint64
	BounceFwdFees            *uint64
	SplitInfoCurShardPfxLen  *int32
	SplitInfoAccSplitDepth   *int32
	SplitInfoThisAddr        *string
	SplitInfoSiblingAddr     *string
	Emulated                 bool
}

type MessageRow struct {
	TxHash               string
	TxLt                 uint64
	MsgHash              string
	Direction            string
	TraceID              *string
	Source               *string
	Destination          *string
	Value                *uint64
	ValueExtraCurrencies map[string]string
	FwdFee               *uint64
	IhrFee               *uint64
	CreatedLt            *uint64
	CreatedAt            *uint32
	Opcode               *int32
	IhrDisabled          *bool
	Bounce               *bool
	Bounced              *bool
	ImportFee            *uint64
	BodyHash             *string
	InitStateHash        *string
	MsgHashNorm          *string
}

type MessageContentRow struct {
	Hash string
	Body *string
}

type TraceRow struct {
	TraceKey            string
	TraceId             *string
	ExternalHash        *string
	McSeqnoStart        uint32
	McSeqnoEnd          uint32
	StartLt             uint64
	StartUtime          uint32
	EndLt               uint64
	EndUtime            uint32
	TraceState          string
	Messages            uint16
	Transactions        uint16
	PendingMessages     uint16
	ClassificationState string
}

type assign func(dest any) error
type assignable interface {
	getAssigns() []assign
}

type genericRow struct {
	assigns []assign
}

func merge(assignables ...assignable) []assign {
	var assigns []assign
	for _, a := range assignables {
		if a != nil {
			assigns = append(assigns, a.getAssigns()...)
		}
	}
	return assigns
}

func NewRow(assignables ...assignable) pgx.Row {
	return genericRow{assigns: merge(assignables...)}
}

func (r genericRow) getAssigns() []assign {
	return r.assigns
}

func (r genericRow) Scan(dest ...any) error {
	for i, d := range dest {
		if i < len(r.assigns) && r.assigns[i] != nil {
			err := r.assigns[i](d)
			if err != nil {
				println("error ", err, " ", i, " ", d)
				return err
			}
		}
	}
	return nil
}

func (t *TraceRow) getAssigns() []assign {
	return []assign{
		assignStringPtr(t.TraceId),
		assignStringPtr(t.ExternalHash),
		assignInt(t.McSeqnoStart),
		assignInt(t.McSeqnoEnd),
		assignInt(t.StartLt),
		assignInt(t.StartUtime),
		assignInt(t.EndLt),
		assignInt(t.EndUtime),
		assignString(t.TraceState),
		assignInt(t.Messages),
		assignInt(t.Transactions),
		assignInt(t.PendingMessages),
		assignString(t.ClassificationState),
	}
}

func (t *TransactionRow) getAssigns() []assign {
	return []assign{
		assignString(t.Account),
		assignString(t.Hash),
		assignInt(t.Lt),
		assignIntPtr(t.BlockWorkchain),
		assignIntPtr(t.BlockShard),
		assignIntPtr(t.BlockSeqno),
		assignIntPtr(t.McBlockSeqno),
		assignStringPtr(t.TraceID),
		assignStringPtr(t.PrevTransHash),
		assignIntPtr(t.PrevTransLt),
		assignIntPtr(t.Now),
		assignStringPtr(t.OrigStatus),
		assignStringPtr(t.EndStatus),
		assignIntPtr(t.TotalFees),
		assignMap(t.TotalFeesExtraCurrencies),
		assignStringPtr(t.AccountStateHashBefore),
		assignStringPtr(t.AccountStateHashAfter),
		assignStringPtr(t.Descr),
		assignBoolPtr(t.Aborted),
		assignBoolPtr(t.Destroyed),
		assignBoolPtr(t.CreditFirst),
		assignBoolPtr(t.IsTock),
		assignBoolPtr(t.Installed),
		assignIntPtr(t.StorageFeesCollected),
		assignIntPtr(t.StorageFeesDue),
		assignStringPtr(t.StorageStatusChange),
		assignIntPtr(t.CreditDueFeesCollected),
		assignIntPtr(t.Credit),
		assignMap(t.CreditExtraCurrencies),
		assignBoolPtr(t.ComputeSkipped),
		assignStringPtr(t.SkippedReason),
		assignBoolPtr(t.ComputeSuccess),
		assignBoolPtr(t.ComputeMsgStateUsed),
		assignBoolPtr(t.ComputeAccountActivated),
		assignIntPtr(t.ComputeGasFees),
		assignIntPtr(t.ComputeGasUsed),
		assignIntPtr(t.ComputeGasLimit),
		assignIntPtr(t.ComputeGasCredit),
		assignIntPtr(t.ComputeMode),
		assignIntPtr(t.ComputeExitCode),
		assignIntPtr(t.ComputeExitArg),
		assignIntPtr(t.ComputeVmSteps),
		assignStringPtr(t.ComputeVmInitStateHash),
		assignStringPtr(t.ComputeVmFinalStateHash),
		assignBoolPtr(t.ActionSuccess),
		assignBoolPtr(t.ActionValid),
		assignBoolPtr(t.ActionNoFunds),
		assignStringPtr(t.ActionStatusChange),
		assignIntPtr(t.ActionTotalFwdFees),
		assignIntPtr(t.ActionTotalActionFees),
		assignIntPtr(t.ActionResultCode),
		assignIntPtr(t.ActionResultArg),
		assignIntPtr(t.ActionTotActions),
		assignIntPtr(t.ActionSpecActions),
		assignIntPtr(t.ActionSkippedActions),
		assignIntPtr(t.ActionMsgsCreated),
		assignStringPtr(t.ActionActionListHash),
		assignIntPtr(t.ActionTotMsgSizeCells),
		assignIntPtr(t.ActionTotMsgSizeBits),
		assignStringPtr(t.Bounce),
		assignIntPtr(t.BounceMsgSizeCells),
		assignIntPtr(t.BounceMsgSizeBits),
		assignIntPtr(t.BounceReqFwdFees),
		assignIntPtr(t.BounceMsgFees),
		assignIntPtr(t.BounceFwdFees),
		assignIntPtr(t.SplitInfoCurShardPfxLen),
		assignIntPtr(t.SplitInfoAccSplitDepth),
		assignStringPtr(t.SplitInfoThisAddr),
		assignStringPtr(t.SplitInfoSiblingAddr),
		assignBool(t.Emulated),
	}
}
func (t *TransactionRow) Scan(dest ...any) error {
	assigns := t.getAssigns()
	for i, d := range dest {
		err := assigns[i](d)
		if err != nil {
			println("error ", err, " ", i, " ", d)
			return err
		}
	}
	return nil
}
func (m *MessageRow) getAssigns() []assign {
	return []assign{
		assignString(m.TxHash),
		assignInt(m.TxLt),
		assignString(m.MsgHash),
		assignString(m.Direction),
		assignStringPtr(m.TraceID),
		assignStringPtr(m.Source),
		assignStringPtr(m.Destination),
		assignIntPtr(m.Value),
		assignMap(m.ValueExtraCurrencies),
		assignIntPtr(m.FwdFee),
		assignIntPtr(m.IhrFee),
		assignIntPtr(m.CreatedLt),
		assignIntPtr(m.CreatedAt),
		assignIntPtr(m.Opcode),
		assignBoolPtr(m.IhrDisabled),
		assignBoolPtr(m.Bounce),
		assignBoolPtr(m.Bounced),
		assignIntPtr(m.ImportFee),
		assignStringPtr(m.BodyHash),
		assignStringPtr(m.InitStateHash),
		assignStringPtr(m.MsgHashNorm),
		assignStringPtr(nil), // InMsgTxHash
		assignStringPtr(nil), // OutMsgTxHash
	}
}

func (c *MessageContentRow) getAssigns() []assign {
	return []assign{
		assignString(c.Hash),
		assignStringPtr(c.Body),
	}
}
func (m *MessageRow) Scan(dest ...any) error {
	assigns := m.getAssigns()
	for i, d := range dest {
		err := assigns[i](d)
		if err != nil {
			return err
		}
	}
	return nil
}
func assignIntPtr[T int64 | int32 | int16 | int8 | uint32 | uint64 | uint16](src *T) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignInt(*src)
}
func assignInt[T int64 | int32 | int16 | int8 | uint16 | uint32 | uint64](src T) assign {
	return func(dest any) error {

		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetInt(int64(src))

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dv.SetUint(uint64(src))

		case reflect.String:
			dv.SetString(strconv.FormatInt(int64(src), 10))

		default:
			return fmt.Errorf("unsupported type %T for %d", dest, src)
		}
		return nil
	}
}
func assignBoolPtr(src *bool) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignBool(*src)
}
func assignBool(src bool) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.Pointer:
			err := assignBool(src)(dv.Interface())
			return err
		case reflect.Bool:
			dv.SetBool(src)
			break
		default:
			return fmt.Errorf("unsupported type %T for %s", dest, src)

		}
		return nil
	}
}
func assignStringPtr(src *string) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignString(*src)
}

func assignMap(src map[string]string) assign {
	return func(dest any) error {
		// Since we know we expect *map[string]string, check for that type.
		m, ok := dest.(*map[string]string)
		if !ok {
			return fmt.Errorf("dest must be of type *map[string]string, but got %T", dest)
		}

		// Assign the src map to the dereferenced map pointer.
		*m = src
		return nil
	}
}
func assignStrCompatibleSlice(src []string) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.Slice:
			elemType := dv.Type().Elem()
			if elemType.Kind() == reflect.String {
				// If the slice element type is string
				slice := reflect.MakeSlice(dv.Type(), len(src), len(src))
				for i, v := range src {
					slice.Index(i).SetString(v)
				}
				dv.Set(slice)
			} else if elemType.Kind() == reflect.Ptr && elemType.Elem().Kind() == reflect.String {
				// If the slice element type is *string
				slice := reflect.MakeSlice(dv.Type(), len(src), len(src))
				for i, v := range src {
					strPtr := reflect.New(elemType.Elem()) // Create a new *string
					strPtr.Elem().SetString(v)             // Set the value of *string
					slice.Index(i).Set(strPtr)             // Assign to the slice
				}
				dv.Set(slice)
			} else {
				return fmt.Errorf("unsupported slice element type %s", elemType.Kind())
			}
		default:
			return fmt.Errorf("unsupported type %T, expected slice of strings or slice of *string", dest)
		}
		return nil
	}
}
func assignString(src string) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.String:
			dv.SetString(src)
			break
		default:
			return fmt.Errorf("unsupported type %T for %s", dest, src)
		}
		return nil
	}
}

func assignStruct(src interface{}) assign {
	return func(dest any) error {
		dv := reflect.ValueOf(dest)
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("destination must be a pointer to a struct")
		}
		dv = dv.Elem()
		if dv.Kind() != reflect.Struct {
			return fmt.Errorf("destination is not a struct")
		}

		sv := reflect.ValueOf(src)
		if sv.Kind() == reflect.Ptr {
			sv = sv.Elem()
		}
		if sv.Kind() != reflect.Struct {
			return fmt.Errorf("source is not a struct")
		}

		srcType := sv.Type()
		for i := 0; i < srcType.NumField(); i++ {
			srcField := sv.Field(i)
			srcFieldName := srcType.Field(i).Name
			destField := dv.FieldByName(srcFieldName)
			if !destField.IsValid() || !destField.CanSet() {
				continue // Skip if field doesn't exist or can't be set
			}
			if srcField.Type().AssignableTo(destField.Type()) {
				destField.Set(srcField)
			} else if srcField.Type().ConvertibleTo(destField.Type()) {
				destField.Set(srcField.Convert(destField.Type()))
			} else {
				return fmt.Errorf("field %s cannot be set", srcFieldName)
			}
		}
		return nil
	}
}

func assignSlice[T any](src []T) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		if dv.Kind() != reflect.Slice {
			return fmt.Errorf("destination is not a slice, got %T", dest)
		}

		slice := reflect.MakeSlice(dv.Type(), len(src), len(src))

		for i, v := range src {
			elem := slice.Index(i)
			if elem.Kind() == reflect.Ptr {
				// Handle pointer elements (e.g., []*DestStruct)
				newElem := reflect.New(elem.Type().Elem())
				if err := assignValue(reflect.ValueOf(v), newElem.Elem()); err != nil {
					return err
				}
				elem.Set(newElem)
			} else {
				// Handle value elements (e.g., []DestStruct)
				if err := assignValue(reflect.ValueOf(v), elem.Addr().Interface()); err != nil {
					return err
				}
			}
		}

		dv.Set(slice)
		return nil
	}
}

func assignValue(src reflect.Value, dest any) error {
	dv := reflect.ValueOf(dest).Elem()
	if src.Type().AssignableTo(dv.Type()) {
		dv.Set(src)
		return nil
	}

	switch dv.Kind() {
	case reflect.Struct:
		return assignStruct(src.Interface())(dest)
	}

	return fmt.Errorf("unsupported assignment from %v to %v", src.Type(), dv.Type())
}
