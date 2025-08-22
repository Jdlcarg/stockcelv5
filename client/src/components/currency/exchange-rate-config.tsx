import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Settings, DollarSign } from "lucide-react";
import { toast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/use-auth";

export default function ExchangeRateConfig() {
  const { user } = useAuth();
  const [isOpen, setIsOpen] = useState(false);
  const [exchangeRateARS, setExchangeRateARS] = useState("1000");

  useEffect(() => {
    // Cargar tasa guardada
    const savedRate = localStorage.getItem(`exchange_rate_ars_usd_${user?.clientId}`) || "1000";
    setExchangeRateARS(savedRate);
  }, [user?.clientId]);

  const saveExchangeRate = () => {
    if (!exchangeRateARS || parseFloat(exchangeRateARS) <= 0) {
      toast({
        title: "Error",
        description: "Ingrese una tasa de cambio válida",
        variant: "destructive"
      });
      return;
    }

    localStorage.setItem(`exchange_rate_ars_usd_${user?.clientId}`, exchangeRateARS);
    
    toast({
      title: "Tasa de cambio actualizada",
      description: `ARS/USD configurado en ${exchangeRateARS}`
    });
    
    setIsOpen(false);
    
    // Enviar evento personalizado para notificar a otros componentes
    window.dispatchEvent(new CustomEvent('exchange-rate-updated', { 
      detail: { rate: exchangeRateARS } 
    }));
  };

  const getCurrentRate = () => {
    return localStorage.getItem(`exchange_rate_ars_usd_${user?.clientId}`) || "1000";
  };

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Settings className="h-4 w-4 mr-2" />
          Configurar TC
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Configurar Tasa de Conversión</DialogTitle>
        </DialogHeader>
        
        <div className="space-y-4">
          <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
            <h4 className="font-medium text-blue-700 dark:text-blue-300 mb-2">
              Tasa Actual
            </h4>
            <p className="text-sm text-blue-600 dark:text-blue-400">
              1 USD = {getCurrentRate()} ARS
            </p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="ars-rate">Nueva Tasa ARS/USD</Label>
            <div className="relative">
              <Input
                id="ars-rate"
                type="number"
                placeholder="1000"
                value={exchangeRateARS}
                onChange={(e) => setExchangeRateARS(e.target.value)}
                className="pl-8"
                min="0"
                step="0.01"
              />
              <DollarSign className="absolute left-2 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500" />
            </div>
            <p className="text-xs text-gray-500">
              Ejemplo: Si 1 USD = 1000 ARS, ingrese 1000
            </p>
          </div>

          <div className="bg-gray-50 dark:bg-gray-800 p-3 rounded">
            <p className="text-sm">
              <strong>Vista previa:</strong> 1 USD = {exchangeRateARS || "0"} ARS
            </p>
          </div>

          <div className="flex gap-2 pt-4">
            <Button variant="outline" onClick={() => setIsOpen(false)} className="flex-1">
              Cancelar
            </Button>
            <Button onClick={saveExchangeRate} className="flex-1">
              Guardar
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}