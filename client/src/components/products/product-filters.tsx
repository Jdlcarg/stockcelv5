import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

export interface ProductFilters {
  model?: string;
  storage?: string;
  status?: string;
  priceMin?: string;
  priceMax?: string;
  provider?: string;
  battery?: string;
  quality?: string;
}

interface ProductFiltersProps {
  filters: ProductFilters;
  onFiltersChange: (filters: ProductFilters) => void;
  onClearFilters: () => void;
  products: any[];
}

export default function ProductFilters({ filters, onFiltersChange, onClearFilters, products }: ProductFiltersProps) {
  const updateFilter = (key: keyof ProductFilters, value: string | undefined) => {
    onFiltersChange({
      ...filters,
      [key]: value === 'all' ? undefined : value,
    });
  };

  // Obtener valores únicos de los productos para los filtros
  const uniqueModels = [...new Set(products.map(p => p.model).filter(Boolean))];
  const uniqueStorages = [...new Set(products.map(p => p.storage).filter(Boolean))];
  const uniqueProviders = [...new Set(products.map(p => p.provider).filter(Boolean))];
  const uniqueBatteries = [...new Set(products.map(p => p.battery).filter(Boolean))];
  const uniqueQualities = [...new Set(products.map(p => p.quality).filter(Boolean))];

  return (
    <div className="p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border">
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        
        {/* Modelo */}
        <div className="space-y-2">
          <Label htmlFor="model-filter">Modelo</Label>
          <Select value={filters.model || 'all'} onValueChange={(value) => updateFilter('model', value)}>
            <SelectTrigger>
              <SelectValue placeholder="Todos los modelos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos los modelos</SelectItem>
              {uniqueModels.map(model => (
                <SelectItem key={model} value={model}>{model}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Almacenamiento */}
        <div className="space-y-2">
          <Label htmlFor="storage-filter">Almacenamiento</Label>
          <Select value={filters.storage || 'all'} onValueChange={(value) => updateFilter('storage', value)}>
            <SelectTrigger>
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos</SelectItem>
              {uniqueStorages.map(storage => (
                <SelectItem key={storage} value={storage}>{storage}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Estado */}
        <div className="space-y-2">
          <Label htmlFor="status-filter">Estado</Label>
          <Select value={filters.status || 'all'} onValueChange={(value) => updateFilter('status', value)}>
            <SelectTrigger>
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos los estados</SelectItem>
              <SelectItem value="disponible">Disponible</SelectItem>
              <SelectItem value="reservado">Reservado</SelectItem>
              <SelectItem value="vendido">Vendido</SelectItem>
              <SelectItem value="tecnico_interno">Técnico Interno</SelectItem>
              <SelectItem value="tecnico_externo">Técnico Externo</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Proveedor */}
        <div className="space-y-2">
          <Label htmlFor="provider-filter">Proveedor</Label>
          <Select value={filters.provider || 'all'} onValueChange={(value) => updateFilter('provider', value)}>
            <SelectTrigger>
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos</SelectItem>
              {uniqueProviders.map(provider => (
                <SelectItem key={provider} value={provider}>{provider}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Batería */}
        <div className="space-y-2">
          <Label htmlFor="battery-filter">Batería</Label>
          <Select value={filters.battery || 'all'} onValueChange={(value) => updateFilter('battery', value)}>
            <SelectTrigger>
              <SelectValue placeholder="Todas" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas</SelectItem>
              {uniqueBatteries.map(battery => (
                <SelectItem key={battery} value={battery}>{battery}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  );
}