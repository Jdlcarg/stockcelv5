import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { DollarSign, ArrowUpDown, Settings, TrendingUp, TrendingDown } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface ExchangeRateEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  exchangeRates: Record<string, number>;
  onUpdateRates: (rates: Record<string, number>) => void;
}

export default function ExchangeRateEditor({ 
  open, 
  onOpenChange, 
  exchangeRates, 
  onUpdateRates 
}: ExchangeRateEditorProps) {
  const { toast } = useToast();
  const [tempRates, setTempRates] = useState(exchangeRates);

  const paymentMethods = [
    { 
      key: "efectivo_dolar", 
      label: "Efectivo Dólar", 
      description: "Pagos en efectivo USD",
      baseRate: 1,
      editable: false
    },
    { 
      key: "efectivo_pesos", 
      label: "Efectivo Pesos", 
      description: "Pagos en efectivo ARS",
      baseRate: 1000,
      editable: true
    },
    { 
      key: "transferencia_pesos", 
      label: "Transferencia Pesos", 
      description: "Transferencias bancarias ARS",
      baseRate: 1000,
      editable: true
    },
    { 
      key: "transferencia_usdt", 
      label: "Transferencia USDT", 
      description: "Transferencias en USDT",
      baseRate: 1,
      editable: false
    },
    { 
      key: "transferencia_financiera", 
      label: "Transferencia Financiera", 
      description: "Transferencias con swap automático",
      baseRate: 1050,
      editable: true
    },
  ];

  const handleRateChange = (key: string, value: string) => {
    const numValue = parseFloat(value);
    if (!isNaN(numValue) && numValue > 0) {
      setTempRates(prev => ({
        ...prev,
        [key]: numValue
      }));
    }
  };

  const resetToDefault = (key: string) => {
    const method = paymentMethods.find(m => m.key === key);
    if (method) {
      setTempRates(prev => ({
        ...prev,
        [key]: method.baseRate
      }));
    }
  };

  const saveRates = () => {
    onUpdateRates(tempRates);
    onOpenChange(false);
    toast({ title: "Tasas de cambio actualizadas exitosamente" });
  };

  const calculateDifference = (current: number, base: number) => {
    const diff = ((current - base) / base) * 100;
    return diff;
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Settings className="h-5 w-5" />
            Configuración de Tasas de Cambio
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div className="text-sm text-gray-600 p-3 bg-blue-50 rounded-lg">
            <strong>Información:</strong> Las tasas de cambio se usan para convertir montos locales a USD. 
            Una tasa de 1000 significa que 1000 pesos = 1 USD.
          </div>

          {paymentMethods.map((method) => {
            const currentRate = tempRates[method.key] || method.baseRate;
            const difference = calculateDifference(currentRate, method.baseRate);
            const isHigher = difference > 0;
            const isLower = difference < 0;

            return (
              <Card key={method.key} className={!method.editable ? "opacity-60" : ""}>
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="text-lg">{method.label}</CardTitle>
                      <p className="text-sm text-gray-600">{method.description}</p>
                    </div>
                    <div className="flex items-center gap-2">
                      {method.editable && Math.abs(difference) > 0.1 && (
                        <Badge variant={isHigher ? "destructive" : "secondary"} className="text-xs">
                          {isHigher ? (
                            <TrendingUp className="h-3 w-3 mr-1" />
                          ) : (
                            <TrendingDown className="h-3 w-3 mr-1" />
                          )}
                          {difference.toFixed(1)}%
                        </Badge>
                      )}
                      {!method.editable && (
                        <Badge variant="outline">Fijo</Badge>
                      )}
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="pt-0">
                  <div className="grid grid-cols-2 gap-4 items-end">
                    <div>
                      <Label htmlFor={`rate-${method.key}`}>
                        Tasa de Cambio
                      </Label>
                      <div className="flex gap-2">
                        <div className="relative flex-1">
                          <DollarSign className="absolute left-2 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500" />
                          <Input
                            id={`rate-${method.key}`}
                            type="number"
                            step="0.01"
                            value={currentRate}
                            onChange={(e) => handleRateChange(method.key, e.target.value)}
                            disabled={!method.editable}
                            className="pl-8"
                            placeholder="0.00"
                          />
                        </div>
                        {method.editable && (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => resetToDefault(method.key)}
                            className="px-3"
                          >
                            <ArrowUpDown className="h-4 w-4" />
                          </Button>
                        )}
                      </div>
                    </div>

                    <div>
                      <Label className="text-sm text-gray-600">
                        Equivalencia
                      </Label>
                      <div className="text-sm font-mono bg-gray-50 p-2 rounded">
                        {method.key === "efectivo_dolar" || method.key === "transferencia_usdt" ? (
                          <>1 USD = 1 USD</>
                        ) : (
                          <>1 USD = {currentRate.toLocaleString()} {method.key.includes("pesos") ? "ARS" : "Unidades"}</>
                        )}
                      </div>
                    </div>
                  </div>

                  {method.editable && (
                    <div className="mt-3 text-xs text-gray-500">
                      <strong>Tasa base:</strong> {method.baseRate.toLocaleString()} | 
                      <strong> Actual:</strong> {currentRate.toLocaleString()} | 
                      <strong> Diferencia:</strong> {difference.toFixed(2)}%
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })}

          <div className="flex justify-end gap-2 pt-4">
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button onClick={saveRates}>
              <DollarSign className="h-4 w-4 mr-2" />
              Guardar Tasas
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}