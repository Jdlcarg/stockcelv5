import CreateOrderModal from "@/components/orders/create-order-modal";

interface ImeiScannerModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function ImeiScannerModal({ open, onOpenChange }: ImeiScannerModalProps) {
  return (
    <CreateOrderModal
      open={open}
      onOpenChange={onOpenChange}
    />
  );
}