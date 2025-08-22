import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { History, Clock, User } from "lucide-react";
import { useAuth } from "@/hooks/use-auth";

interface ProductHistoryModalProps {
  isOpen: boolean;
  onClose: () => void;
  productId: number;
  productModel: string;
}

export function ProductHistoryModal({ isOpen, onClose, productId, productModel }: ProductHistoryModalProps) {
  const { user } = useAuth();
  
  const { data: history, isLoading } = useQuery({
    queryKey: ["/api/products", productId, "history"],
    enabled: isOpen && !!productId,
  });

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "disponible":
        return "bg-green-100 text-green-800";
      case "reservado":
        return "bg-yellow-100 text-yellow-800";
      case "vendido":
        return "bg-blue-100 text-blue-800";
      case "tecnico_interno":
        return "bg-orange-100 text-orange-800";
      case "tecnico_externo":
        return "bg-red-100 text-red-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const formatStatus = (status: string) => {
    switch (status.toLowerCase()) {
      case "disponible":
        return "Disponible";
      case "reservado":
        return "Reservado";
      case "vendido":
        return "Vendido";
      case "tecnico_interno":
        return "Técnico Interno";
      case "tecnico_externo":
        return "Técnico Externo";
      default:
        return status;
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <History className="h-5 w-5" />
            Historial del Producto - {productModel}
          </DialogTitle>
        </DialogHeader>

        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          </div>
        ) : (
          <ScrollArea className="max-h-96">
            <div className="space-y-4">
              {history && history.length > 0 ? (
                history.map((entry: any) => (
                  <div key={entry.id} className="border rounded-lg p-4 bg-gray-50">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Clock className="h-4 w-4 text-gray-500" />
                        <span className="text-sm text-gray-600">
                          {format(new Date(entry.createdAt), "dd/MM/yyyy HH:mm")}
                        </span>
                      </div>
                      <div className="flex items-center gap-2">
                        <User className="h-4 w-4 text-gray-500" />
                        <span className="text-sm text-gray-600">
                          Usuario ID: {entry.userId}
                        </span>
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-2 mb-2">
                      {entry.previousStatus && (
                        <>
                          <Badge className={getStatusColor(entry.previousStatus)}>
                            {formatStatus(entry.previousStatus)}
                          </Badge>
                          <span className="text-gray-500">→</span>
                        </>
                      )}
                      <Badge className={getStatusColor(entry.newStatus)}>
                        {formatStatus(entry.newStatus)}
                      </Badge>
                    </div>
                    
                    {entry.notes && (
                      <p className="text-sm text-gray-700 mt-2">
                        {entry.notes}
                      </p>
                    )}
                  </div>
                ))
              ) : (
                <div className="text-center py-8 text-gray-500">
                  No hay historial disponible para este producto
                </div>
              )}
            </div>
          </ScrollArea>
        )}

        <div className="flex justify-end">
          <Button variant="outline" onClick={onClose}>
            Cerrar
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}