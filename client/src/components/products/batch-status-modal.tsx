import { useState, useRef, useEffect } from "react";
import { useMutation, useQueryClient, useQuery } from "@tanstack/react-query";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/lib/auth";
import { apiRequest } from "@/lib/queryClient";
import { X, CheckCircle, XCircle, Loader2 } from "lucide-react";

interface BatchStatusModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function BatchStatusModal({ open, onOpenChange }: BatchStatusModalProps) {
  const { toast } = useToast();
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const inputRef = useRef<HTMLInputElement>(null);
  
  const [scannedImeis, setScannedImeis] = useState<string[]>([]);
  const [currentImei, setCurrentImei] = useState("");
  const [formData, setFormData] = useState({
    status: "",
    observations: "",
  });

  // Obtener productos para validar IMEIs
  const { data: products } = useQuery({
    queryKey: ["/api/products", user?.clientId],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (user?.clientId) params.append("clientId", user.clientId.toString());
      const response = await fetch(`/api/products?${params}`);
      return response.json();
    },
    enabled: !!user?.clientId && open,
  });

  // Enfocar el input cuando se abre el modal
  useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open]);

  // Limpiar datos cuando se cierra el modal
  useEffect(() => {
    if (!open) {
      setScannedImeis([]);
      setCurrentImei("");
      setFormData({ status: "", observations: "" });
    }
  }, [open]);

  // Función para validar si un IMEI existe en el inventario
  const validateImei = (imei: string): boolean => {
    if (!products || !imei.trim()) return false;
    return products.some((product: any) => product.imei === imei.trim());
  };

  // Función para agregar un IMEI escaneado
  const addScannedImei = (imei: string) => {
    const trimmedImei = imei.trim();
    
    if (!trimmedImei) return;
    
    if (!validateImei(trimmedImei)) {
      toast({
        title: "IMEI no encontrado",
        description: `El IMEI ${trimmedImei} no existe en tu inventario`,
        variant: "destructive",
      });
      return;
    }
    
    if (scannedImeis.includes(trimmedImei)) {
      toast({
        title: "IMEI duplicado",
        description: `El IMEI ${trimmedImei} ya fue escaneado`,
        variant: "destructive",
      });
      return;
    }
    
    setScannedImeis(prev => [...prev, trimmedImei]);
    setCurrentImei("");
    
    // Enfocar el input nuevamente
    if (inputRef.current) {
      inputRef.current.focus();
    }
  };

  // Función para remover un IMEI escaneado
  const removeScannedImei = (imei: string) => {
    setScannedImeis(prev => prev.filter(item => item !== imei));
  };

  // Manejar el Enter en el input
  const handleImeiKeyPress = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addScannedImei(currentImei);
    }
  };

  const batchUpdateMutation = useMutation({
    mutationFn: async (data: any) => {
      return await apiRequest('PUT', '/api/products/batch-update', {
        imeis: scannedImeis,
        clientId: user?.clientId,
        userId: user?.id,
        updates: {
          status: data.status,
          observations: data.observations || null,
        }
      });
    },
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      
      const { success, notFound } = result;
      
      if (success > 0) {
        toast({
          title: "Éxito",
          description: `${success} productos actualizados correctamente`,
        });
      }
      
      if (notFound && notFound.length > 0) {
        toast({
          title: "Advertencia",
          description: `${notFound.length} IMEI(s) no encontrados: ${notFound.join(', ')}`,
          variant: "destructive",
        });
      }
      
      setScannedImeis([]);
      setCurrentImei("");
      setFormData({ status: "", observations: "" });
      onOpenChange(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo actualizar los productos",
        variant: "destructive",
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (scannedImeis.length === 0) {
      toast({
        title: "Error",
        description: "Debes escanear al menos un IMEI",
        variant: "destructive",
      });
      return;
    }
    
    if (!formData.status) {
      toast({
        title: "Error",
        description: "Debes seleccionar un estado",
        variant: "destructive",
      });
      return;
    }
    
    batchUpdateMutation.mutate(formData);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Cambio Masivo de Estado</DialogTitle>
        </DialogHeader>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="imei-scanner">Escanear IMEIs</Label>
            <Input
              ref={inputRef}
              id="imei-scanner"
              placeholder="Escanea o ingresa un IMEI y presiona Enter"
              value={currentImei}
              onChange={(e) => setCurrentImei(e.target.value)}
              onKeyPress={handleImeiKeyPress}
              className="text-lg"
            />
            <p className="text-sm text-gray-500">
              Escanea cada IMEI y presiona Enter. Solo procesará IMEIs existentes en tu inventario.
            </p>
          </div>

          {/* Lista de IMEIs escaneados */}
          <div className="space-y-2">
            <Label>IMEIs Escaneados ({scannedImeis.length})</Label>
            <div className="min-h-[120px] max-h-[200px] overflow-y-auto border rounded-md p-2 space-y-1">
              {scannedImeis.length === 0 ? (
                <p className="text-gray-500 text-sm">No hay IMEIs escaneados</p>
              ) : (
                scannedImeis.map((imei) => (
                  <div key={imei} className="flex items-center justify-between bg-gray-50 p-2 rounded">
                    <span className="font-mono text-sm">{imei}</span>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      onClick={() => removeScannedImei(imei)}
                      className="text-red-500 hover:text-red-700"
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="status">Nuevo Estado</Label>
            <Select value={formData.status} onValueChange={(value) => setFormData({ ...formData, status: value })}>
              <SelectTrigger>
                <SelectValue placeholder="Selecciona el nuevo estado" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="disponible">Disponible</SelectItem>
                <SelectItem value="reservado">Reservado</SelectItem>
                <SelectItem value="tecnico_interno">Técnico Interno</SelectItem>
                <SelectItem value="tecnico_externo">Técnico Externo</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="observations">Observaciones (opcional)</Label>
            <Textarea
              id="observations"
              placeholder="Ej: Cambio de pantalla, Reparación de batería, etc."
              value={formData.observations}
              onChange={(e) => setFormData({ ...formData, observations: e.target.value })}
              rows={3}
            />
          </div>

          <div className="flex justify-end space-x-2">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button type="submit" disabled={batchUpdateMutation.isPending}>
              {batchUpdateMutation.isPending ? "Actualizando..." : "Actualizar Productos"}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}