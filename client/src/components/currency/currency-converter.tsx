import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuthStore } from "@/stores/authStore";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ArrowRightLeft, RefreshCw } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { apiRequest } from "@/lib/queryClient";
import { toast } from "@/hooks/use-toast";

const exchangeSchema = z.object({
  fromCurrency: z.string().min(1, "Moneda origen requerida"),
  toCurrency: z.string().min(1, "Moneda destino requerida"),
  fromAmount: z.string().min(1, "Monto requerido"),
  exchangeRate: z.string().min(1, "Cotización requerida"),
  category: z.string().optional(),
  notes: z.string().optional(),
});

interface CurrencyConverterProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function CurrencyConverter({ open, onOpenChange }: CurrencyConverterProps) {
  const { user } = useAuthStore();
  const queryClient = useQueryClient();
  
  const [fromAmount, setFromAmount] = useState("");
  const [toAmount, setToAmount] = useState("");
  const [exchangeRate, setExchangeRate] = useState("");

  const form = useForm({
    resolver: zodResolver(exchangeSchema),
    defaultValues: {
      fromCurrency: "",
      toCurrency: "",
      fromAmount: "",
      exchangeRate: "",
      category: "conversion",
      notes: "",
    },
  });

  const currencies = [
    { value: "usd", label: "USD - Dólares", symbol: "$" },
    { value: "ars", label: "ARS - Pesos Argentinos", symbol: "$" },
    { value: "usdt", label: "USDT - Tether", symbol: "₮" },
  ];

  const calculateExchange = () => {
    const fromAmountNum = parseFloat(fromAmount);
    const rateNum = parseFloat(exchangeRate);
    
    if (fromAmountNum && rateNum) {
      const result = fromAmountNum / rateNum;
      setToAmount(result.toFixed(2));
    }
  };

  const createExchangeMutation = useMutation({
    mutationFn: async (data: any) => {
      return await apiRequest("POST", "/api/currency-exchanges", {
        ...data,
        clientId: user?.clientId,
        userId: user?.id,
        fromAmount: fromAmount,
        toAmount: toAmount,
        exchangeRate: exchangeRate,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/currency-exchanges"] });
      onOpenChange(false);
      form.reset();
      setFromAmount("");
      setToAmount("");
      setExchangeRate("");
      toast({ title: "Conversión registrada exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al registrar la conversión", variant: "destructive" });
    },
  });

  const onSubmit = (data: any) => {
    if (!fromAmount || !toAmount || !exchangeRate) {
      toast({ title: "Complete todos los campos", variant: "destructive" });
      return;
    }

    createExchangeMutation.mutate(data);
  };

  const resetForm = () => {
    form.reset();
    setFromAmount("");
    setToAmount("");
    setExchangeRate("");
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <ArrowRightLeft className="h-5 w-5" />
            Conversión de Moneda
          </DialogTitle>
        </DialogHeader>

        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label>Moneda Origen</Label>
              <Select
                value={form.watch("fromCurrency")}
                onValueChange={(value) => form.setValue("fromCurrency", value)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Seleccionar" />
                </SelectTrigger>
                <SelectContent>
                  {currencies.map(currency => (
                    <SelectItem key={currency.value} value={currency.value}>
                      {currency.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <Label>Moneda Destino</Label>
              <Select
                value={form.watch("toCurrency")}
                onValueChange={(value) => form.setValue("toCurrency", value)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Seleccionar" />
                </SelectTrigger>
                <SelectContent>
                  {currencies.map(currency => (
                    <SelectItem key={currency.value} value={currency.value}>
                      {currency.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div>
            <Label>Monto a Convertir</Label>
            <Input
              type="number"
              step="0.01"
              value={fromAmount}
              onChange={(e) => setFromAmount(e.target.value)}
              placeholder="0.00"
            />
          </div>

          <div>
            <Label>Cotización</Label>
            <div className="flex gap-2">
              <Input
                type="number"
                step="0.0001"
                value={exchangeRate}
                onChange={(e) => setExchangeRate(e.target.value)}
                placeholder="1.0000"
              />
              <Button type="button" onClick={calculateExchange} size="sm">
                <RefreshCw className="h-4 w-4" />
              </Button>
            </div>
          </div>

          <div>
            <Label>Monto Recibido</Label>
            <Input
              type="number"
              value={toAmount}
              onChange={(e) => setToAmount(e.target.value)}
              placeholder="0.00"
            />
          </div>

          <div>
            <Label>Categoría</Label>
            <Select
              value={form.watch("category")}
              onValueChange={(value) => form.setValue("category", value)}
            >
              <SelectTrigger>
                <SelectValue placeholder="Seleccionar categoría" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="conversion">Conversión de Moneda</SelectItem>
                <SelectItem value="purchase">Compra</SelectItem>
                <SelectItem value="sale">Venta</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div>
            <Label>Notas (opcional)</Label>
            <Input
              value={form.watch("notes")}
              onChange={(e) => form.setValue("notes", e.target.value)}
              placeholder="Observaciones..."
            />
          </div>

          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={resetForm}>
              Limpiar
            </Button>
            <Button type="submit" disabled={createExchangeMutation.isPending}>
              {createExchangeMutation.isPending ? "Registrando..." : "Registrar Conversión"}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}