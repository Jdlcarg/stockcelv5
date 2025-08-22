import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Product } from "@shared/schema";
import { apiRequest } from "@/lib/queryClient";
import { AlertTriangle } from "lucide-react";

interface DeleteProductModalProps {
  product: Product;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function DeleteProductModal({ product, open, onOpenChange }: DeleteProductModalProps) {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const deleteProductMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('DELETE', `/api/products/${product.id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      toast({
        title: "Éxito",
        description: "Producto eliminado correctamente",
      });
      onOpenChange(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo eliminar el producto",
        variant: "destructive",
      });
    },
  });

  const handleDelete = () => {
    deleteProductMutation.mutate();
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-red-500" />
            Eliminar Producto
          </DialogTitle>
        </DialogHeader>
        
        <div className="py-4">
          <p className="text-sm text-gray-600 dark:text-gray-300">
            ¿Estás seguro de que deseas eliminar este producto?
          </p>
          
          <div className="mt-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
            <div className="space-y-2 text-gray-900 dark:text-gray-100">
              <p><strong>IMEI:</strong> {product.imei}</p>
              <p><strong>Modelo:</strong> {product.model}</p>
              <p><strong>Almacenamiento:</strong> {product.storage}</p>
              <p><strong>Color:</strong> {product.color}</p>
              <p><strong>Precio:</strong> ${parseFloat(product.priceUsd).toFixed(2)}</p>
              {product.provider && <p><strong>Proveedor:</strong> {product.provider}</p>}
            </div>
          </div>
          
          <p className="mt-4 text-sm text-red-600 dark:text-red-400 font-medium">
            Esta acción no se puede deshacer.
          </p>
        </div>
        
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancelar
          </Button>
          <Button 
            variant="destructive" 
            onClick={handleDelete}
            disabled={deleteProductMutation.isPending}
          >
            {deleteProductMutation.isPending ? "Eliminando..." : "Eliminar"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}