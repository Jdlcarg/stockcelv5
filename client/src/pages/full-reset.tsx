import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Trash2, AlertTriangle, CheckCircle, Loader2 } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { Checkbox } from "@/components/ui/checkbox";

export default function FullReset() {
  const [isResetting, setIsResetting] = useState(false);
  const [resetCompleted, setResetCompleted] = useState(false);
  const [confirmReset, setConfirmReset] = useState(false);
  const { toast } = useToast();

  const performFullReset = async () => {
    if (!confirmReset) {
      toast({
        title: "Confirmación requerida",
        description: "Debes confirmar la casilla para proceder con el reset",
        variant: "destructive"
      });
      return;
    }

    setIsResetting(true);
    try {
      const response = await fetch('/api/system/full-reset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirmed: true })
      });

      if (response.ok) {
        setResetCompleted(true);
        toast({
          title: "Reset Completado",
          description: "Todos los datos han sido eliminados y el sistema está listo para testing",
          variant: "default"
        });
      } else {
        throw new Error('Error en el reset');
      }
    } catch (error) {
      toast({
        title: "Error en el Reset",
        description: "Ocurrió un error durante el proceso de reset",
        variant: "destructive"
      });
    } finally {
      setIsResetting(false);
    }
  };

  return (
    <div className="container max-w-2xl mx-auto py-8 px-4">
      <Card className="border-red-200 bg-red-50 dark:bg-red-950/20 dark:border-red-800">
        <CardHeader>
          <div className="flex items-center space-x-2">
            <AlertTriangle className="h-6 w-6 text-red-600" />
            <CardTitle className="text-red-700 dark:text-red-300">
              Reset Completo del Sistema
            </CardTitle>
          </div>
          <CardDescription className="text-red-600 dark:text-red-400">
            Esta acción eliminará TODOS los datos del sistema para permitir testing limpio
          </CardDescription>
        </CardHeader>
        
        <CardContent className="space-y-6">
          <Alert className="border-yellow-200 bg-yellow-50 dark:bg-yellow-950/20">
            <AlertTriangle className="h-4 w-4 text-yellow-600" />
            <AlertTitle className="text-yellow-700 dark:text-yellow-300">
              ⚠️ ADVERTENCIA CRÍTICA
            </AlertTitle>
            <AlertDescription className="text-yellow-600 dark:text-yellow-400">
              Esta acción es IRREVERSIBLE y eliminará:
              <ul className="list-disc list-inside mt-2 space-y-1">
                <li>Todas las órdenes y sus items</li>
                <li>Todos los pagos y movimientos de caja</li>
                <li>Todas las deudas de clientes</li>
                <li>Todos los gastos registrados</li>
                <li>Todos los productos (excepto estructura básica)</li>
                <li>Todo el historial de cambios de estado</li>
              </ul>
            </AlertDescription>
          </Alert>

          {resetCompleted ? (
            <Alert className="border-green-200 bg-green-50 dark:bg-green-950/20">
              <CheckCircle className="h-4 w-4 text-green-600" />
              <AlertTitle className="text-green-700 dark:text-green-300">
                Reset Completado Exitosamente
              </AlertTitle>
              <AlertDescription className="text-green-600 dark:text-green-400">
                El sistema ha sido limpiado completamente y está listo para testing.
                La caja registradora se ha iniciado con balance cero en todas las monedas.
              </AlertDescription>
            </Alert>
          ) : (
            <>
              <div className="flex items-center space-x-2 p-4 border rounded-lg bg-gray-50 dark:bg-gray-900">
                <Checkbox
                  id="confirm-reset"
                  checked={confirmReset}
                  onCheckedChange={(checked) => setConfirmReset(checked as boolean)}
                />
                <label 
                  htmlFor="confirm-reset" 
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Confirmo que entiendo que esta acción eliminará TODOS los datos y es irreversible
                </label>
              </div>

              <div className="flex flex-col space-y-3">
                <Button
                  onClick={performFullReset}
                  disabled={!confirmReset || isResetting}
                  variant="destructive"
                  size="lg"
                  className="w-full"
                >
                  {isResetting ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Realizando Reset...
                    </>
                  ) : (
                    <>
                      <Trash2 className="mr-2 h-4 w-4" />
                      EJECUTAR RESET COMPLETO
                    </>
                  )}
                </Button>
                
                <Button
                  onClick={() => window.history.back()}
                  variant="outline"
                  size="lg"
                  className="w-full"
                  disabled={isResetting}
                >
                  Cancelar y Volver
                </Button>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}