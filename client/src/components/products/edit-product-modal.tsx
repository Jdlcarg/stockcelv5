import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/hooks/use-toast";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { Product } from "@shared/schema";
import { apiRequest } from "@/lib/queryClient";

interface EditProductModalProps {
  product: Product;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function EditProductModal({ product, open, onOpenChange }: EditProductModalProps) {
  const { toast } = useToast();
  const { user } = useAuth();
  const queryClient = useQueryClient();
  
  const [formData, setFormData] = useState({
    imei: product.imei,
    model: product.model,
    storage: product.storage,
    color: product.color,
    quality: product.quality || "A",
    battery: product.battery || "-80%",
    costPrice: product.costPrice,
    provider: product.provider || "",
    status: product.status,
    observations: product.observations || "",
  });

  const updateProductMutation = useMutation({
    mutationFn: async (data: any) => {
      return await apiRequest('PUT', `/api/products/${product.id}`, {
        ...data,
        userId: user?.id
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      queryClient.invalidateQueries({ queryKey: ['/api/products', product.id, 'history'] });
      toast({
        title: "Éxito",
        description: "Producto actualizado correctamente",
      });
      onOpenChange(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo actualizar el producto",
        variant: "destructive",
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.imei || !formData.model || !formData.storage || !formData.color || !formData.costPrice) {
      toast({
        title: "Error",
        description: "Por favor completa todos los campos obligatorios",
        variant: "destructive",
      });
      return;
    }

    updateProductMutation.mutate({
      ...formData,
      clientId: user?.clientId,
      costPrice: formData.costPrice,
      observations: formData.observations || null,
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Editar Producto</DialogTitle>
        </DialogHeader>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="imei">IMEI *</Label>
              <Input
                id="imei"
                value={formData.imei}
                onChange={(e) => setFormData({ ...formData, imei: e.target.value })}
                placeholder="Ingresa el IMEI"
                required
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="model">Modelo *</Label>
              <Select value={formData.model} onValueChange={(value) => setFormData({ ...formData, model: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona modelo" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="iPhone (Original)">iPhone (Original)</SelectItem>
                  <SelectItem value="iPhone 3G">iPhone 3G</SelectItem>
                  <SelectItem value="iPhone 3GS">iPhone 3GS</SelectItem>
                  <SelectItem value="iPhone 4">iPhone 4</SelectItem>
                  <SelectItem value="iPhone 4S">iPhone 4S</SelectItem>
                  <SelectItem value="iPhone 5">iPhone 5</SelectItem>
                  <SelectItem value="iPhone 5c">iPhone 5c</SelectItem>
                  <SelectItem value="iPhone 5s">iPhone 5s</SelectItem>
                  <SelectItem value="iPhone 6">iPhone 6</SelectItem>
                  <SelectItem value="iPhone 6 Plus">iPhone 6 Plus</SelectItem>
                  <SelectItem value="iPhone 6s">iPhone 6s</SelectItem>
                  <SelectItem value="iPhone 6s Plus">iPhone 6s Plus</SelectItem>
                  <SelectItem value="iPhone SE (1st Gen)">iPhone SE (1st Gen)</SelectItem>
                  <SelectItem value="iPhone 7">iPhone 7</SelectItem>
                  <SelectItem value="iPhone 7 Plus">iPhone 7 Plus</SelectItem>
                  <SelectItem value="iPhone 8">iPhone 8</SelectItem>
                  <SelectItem value="iPhone 8 Plus">iPhone 8 Plus</SelectItem>
                  <SelectItem value="iPhone X">iPhone X</SelectItem>
                  <SelectItem value="iPhone XR">iPhone XR</SelectItem>
                  <SelectItem value="iPhone XS">iPhone XS</SelectItem>
                  <SelectItem value="iPhone XS Max">iPhone XS Max</SelectItem>
                  <SelectItem value="iPhone 11">iPhone 11</SelectItem>
                  <SelectItem value="iPhone 11 Pro">iPhone 11 Pro</SelectItem>
                  <SelectItem value="iPhone 11 Pro Max">iPhone 11 Pro Max</SelectItem>
                  <SelectItem value="iPhone SE (2nd Gen)">iPhone SE (2nd Gen)</SelectItem>
                  <SelectItem value="iPhone 12 Mini">iPhone 12 Mini</SelectItem>
                  <SelectItem value="iPhone 12">iPhone 12</SelectItem>
                  <SelectItem value="iPhone 12 Pro">iPhone 12 Pro</SelectItem>
                  <SelectItem value="iPhone 12 Pro Max">iPhone 12 Pro Max</SelectItem>
                  <SelectItem value="iPhone 13 Mini">iPhone 13 Mini</SelectItem>
                  <SelectItem value="iPhone 13">iPhone 13</SelectItem>
                  <SelectItem value="iPhone 13 Pro">iPhone 13 Pro</SelectItem>
                  <SelectItem value="iPhone 13 Pro Max">iPhone 13 Pro Max</SelectItem>
                  <SelectItem value="iPhone SE (3rd Gen)">iPhone SE (3rd Gen)</SelectItem>
                  <SelectItem value="iPhone 14">iPhone 14</SelectItem>
                  <SelectItem value="iPhone 14 Plus">iPhone 14 Plus</SelectItem>
                  <SelectItem value="iPhone 14 Pro">iPhone 14 Pro</SelectItem>
                  <SelectItem value="iPhone 14 Pro Max">iPhone 14 Pro Max</SelectItem>
                  <SelectItem value="iPhone 15">iPhone 15</SelectItem>
                  <SelectItem value="iPhone 15 Plus">iPhone 15 Plus</SelectItem>
                  <SelectItem value="iPhone 15 Pro">iPhone 15 Pro</SelectItem>
                  <SelectItem value="iPhone 15 Pro Max">iPhone 15 Pro Max</SelectItem>
                  <SelectItem value="iPhone 16">iPhone 16</SelectItem>
                  <SelectItem value="iPhone 16 Plus">iPhone 16 Plus</SelectItem>
                  <SelectItem value="iPhone 16 Pro">iPhone 16 Pro</SelectItem>
                  <SelectItem value="iPhone 16 Pro Max">iPhone 16 Pro Max</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="storage">Almacenamiento *</Label>
              <Select value={formData.storage} onValueChange={(value) => setFormData({ ...formData, storage: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona almacenamiento" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="8GB">8GB</SelectItem>
                  <SelectItem value="16GB">16GB</SelectItem>
                  <SelectItem value="32GB">32GB</SelectItem>
                  <SelectItem value="64GB">64GB</SelectItem>
                  <SelectItem value="128GB">128GB</SelectItem>
                  <SelectItem value="256GB">256GB</SelectItem>
                  <SelectItem value="512GB">512GB</SelectItem>
                  <SelectItem value="1TB">1TB</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="color">Color *</Label>
              <Select value={formData.color} onValueChange={(value) => setFormData({ ...formData, color: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona color" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="Negro">Negro</SelectItem>
                  <SelectItem value="Blanco">Blanco</SelectItem>
                  <SelectItem value="Gris">Gris</SelectItem>
                  <SelectItem value="Oro">Oro</SelectItem>
                  <SelectItem value="Rosa">Rosa</SelectItem>
                  <SelectItem value="Azul">Azul</SelectItem>
                  <SelectItem value="Verde">Verde</SelectItem>
                  <SelectItem value="Morado">Morado</SelectItem>
                  <SelectItem value="Rojo">Rojo</SelectItem>
                  <SelectItem value="Amarillo">Amarillo</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="quality">Calidad</Label>
              <Select value={formData.quality} onValueChange={(value) => setFormData({ ...formData, quality: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona calidad" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="A">A</SelectItem>
                  <SelectItem value="AB">AB</SelectItem>
                  <SelectItem value="B">B</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="battery">Batería</Label>
              <Select value={formData.battery} onValueChange={(value) => setFormData({ ...formData, battery: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona batería" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="-80%">-80%</SelectItem>
                  <SelectItem value="entre 80% y 90%">entre 80% y 90%</SelectItem>
                  <SelectItem value="+90%">+90%</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="costPrice">Precio Costo *</Label>
              <Input
                id="costPrice"
                type="number"
                step="0.01"
                value={formData.costPrice}
                onChange={(e) => setFormData({ ...formData, costPrice: e.target.value })}
                placeholder="0.00"
                required
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="provider">Proveedor</Label>
              <Input
                id="provider"
                value={formData.provider}
                onChange={(e) => setFormData({ ...formData, provider: e.target.value })}
                placeholder="Nombre del proveedor"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="status">Estado</Label>
              <Select value={formData.status} onValueChange={(value) => setFormData({ ...formData, status: value })}>
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona estado" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="disponible">Disponible</SelectItem>
                  <SelectItem value="reservado">Reservado</SelectItem>
                  <SelectItem value="tecnico_interno">Técnico Interno</SelectItem>
                  <SelectItem value="tecnico_externo">Técnico Externo</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          
          <div className="space-y-2">
            <Label htmlFor="observations">Observaciones</Label>
            <Textarea
              id="observations"
              value={formData.observations}
              onChange={(e) => setFormData({ ...formData, observations: e.target.value })}
              placeholder="Observaciones adicionales..."
              rows={3}
            />
          </div>
          
          <div className="flex justify-end space-x-2">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button type="submit" disabled={updateProductMutation.isPending}>
              {updateProductMutation.isPending ? "Actualizando..." : "Actualizar Producto"}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}