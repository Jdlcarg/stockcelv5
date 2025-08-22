import { useState } from "react";
import { useAuth } from "@/hooks/use-auth";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { Plus, ArrowUp, ArrowDown } from "lucide-react";

export default function QuickActions() {
  const [customerName, setCustomerName] = useState("");
  const [imeiScan, setImeiScan] = useState("");
  const [paymentMethod, setPaymentMethod] = useState("usd");
  const [amount, setAmount] = useState("");
  
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const createOrderMutation = useMutation({
    mutationFn: async (orderData: any) => {
      return apiRequest("POST", "/api/orders", orderData);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/orders"] });
      queryClient.invalidateQueries({ queryKey: ["/api/dashboard/stats"] });
      toast({
        title: "Pedido creado",
        description: "El pedido se ha creado exitosamente",
      });
      // Reset form
      setCustomerName("");
      setImeiScan("");
      setAmount("");
    },
    onError: () => {
      toast({
        title: "Error",
        description: "No se pudo crear el pedido",
        variant: "destructive",
      });
    },
  });

  const handleCreateOrder = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!customerName || !imeiScan || !amount) {
      toast({
        title: "Error",
        description: "Por favor completa todos los campos",
        variant: "destructive",
      });
      return;
    }

    createOrderMutation.mutate({
      clientId: user?.clientId,
      customerName,
      totalUsd: parseFloat(amount),
      vendorId: user?.id,
      status: "pendiente",
      paymentStatus: "pendiente",
    });
  };

  return (
    <>
      {/* Sales Order Creation */}
      <Card>
        <CardHeader>
          <CardTitle>Crear Pedido Rápido</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleCreateOrder} className="space-y-4">
            <div>
              <Label htmlFor="customer">Cliente</Label>
              <Input
                id="customer"
                placeholder="Nombre del cliente"
                value={customerName}
                onChange={(e) => setCustomerName(e.target.value)}
              />
            </div>
            
            <div>
              <Label htmlFor="imei-scan">Escanear IMEI</Label>
              <Input
                id="imei-scan"
                placeholder="123456789012345"
                value={imeiScan}
                onChange={(e) => setImeiScan(e.target.value)}
              />
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="payment-method">Forma de Pago</Label>
                <Select value={paymentMethod} onValueChange={setPaymentMethod}>
                  <SelectTrigger>
                    <SelectValue placeholder="Seleccionar" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="usd">USD</SelectItem>
                    <SelectItem value="ars">ARS</SelectItem>
                    <SelectItem value="usdt">USDT</SelectItem>
                    <SelectItem value="financiera">Financiera</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              <div>
                <Label htmlFor="amount">Monto</Label>
                <Input
                  id="amount"
                  type="number"
                  placeholder="0.00"
                  value={amount}
                  onChange={(e) => setAmount(e.target.value)}
                />
              </div>
            </div>
            
            <Button type="submit" className="w-full" disabled={createOrderMutation.isPending}>
              {createOrderMutation.isPending ? "Creando..." : "Crear Pedido"}
            </Button>
          </form>
        </CardContent>
      </Card>

      {/* Cash Register */}
      <Card>
        <CardHeader>
          <CardTitle>Estado de Caja</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Saldo inicial USD:</span>
              <span className="text-sm font-medium text-gray-900">$2,500.00</span>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Saldo inicial ARS:</span>
              <span className="text-sm font-medium text-gray-900">$850,000.00</span>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Ventas del día:</span>
              <span className="text-sm font-medium text-accent">$3,250.00</span>
            </div>
            
            <div className="flex justify-between items-center pt-4 border-t border-gray-200">
              <span className="text-sm font-medium text-gray-900">Saldo actual:</span>
              <span className="text-sm font-bold text-primary">$5,750.00</span>
            </div>
            
            <div className="grid grid-cols-2 gap-3 mt-4">
              <Button className="bg-accent hover:bg-accent/90">
                <ArrowUp className="mr-2 h-4 w-4" />
                Ingreso
              </Button>
              <Button className="bg-red-600 hover:bg-red-600/90">
                <ArrowDown className="mr-2 h-4 w-4" />
                Egreso
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </>
  );
}
