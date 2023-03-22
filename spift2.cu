#include <thrust/complex.h>
#define PI 3.14159265358979323846264338327950288f
#define TILE_SIZE 32
typedef thrust::complex<double> complex;

extern "C"{
__global__ void computeVec(
    int N, complex *dstVec,
    int u, int v, complex vis, bool isCS)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < N) {
        if (isCS) {
            int m = idx * u % N;
            complex w = complex(cos(2 * PI * m / N), sin(2 * PI * m / N));
            dstVec[idx] += vis * w;
        }
        else {
            int m = idx * v % N;
            complex w = complex(cos(2 * PI * m / N), sin(2 * PI * m / N));
            dstVec[idx] += vis * w;
        }
    }
}

__global__ void incUpdate(
    int N, complex *I, complex *q, bool isCS, int p)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    int idx = -1;

    if (isCS) {
        int j = i / N;
        int k = i % N;

        int startIdx = (p * k) % N;
        idx = (startIdx + j) % N;
    }
    else {
        int j = i / N;
        int k = i % N;

        int startIdx = (p * j) % N;
        idx = (startIdx + k) % N;
    }

    I[i] +=  q[idx];
}

__global__ void rowUpdate(
    int N, complex *resMat, complex* accVec, int shiftIdx
)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    complex val = accVec[idx];
    shiftIdx = N - shiftIdx;

    int offset = 0;
    for (int row = 0; row < N; row++)
    {
        resMat[row * N + (idx + offset) % N] += val;
        offset += shiftIdx;
    }
}

__global__ void rowUpdate2(
    int N, complex *resMat, complex* accVec, int shiftIdx
)
{
    int x = blockIdx.x * blockDim.x + threadIdx.x;
    int y = blockIdx.y * blockDim.y + threadIdx.y;

    int offset = y * shiftIdx;
    resMat[y * N + x] += accVec[(x + offset) % N];
}

__global__ void colUpdate(
    int N, complex *resMat, complex* accVec, int shiftIdx
)
{
    __shared__ complex tile[TILE_SIZE][TILE_SIZE];

    int blockx = blockIdx.x * blockDim.x;
    int blocky = blockIdx.y * blockDim.y;

    int x = blockx + threadIdx.y;
    int y = blocky + threadIdx.x;

    int offset = x * shiftIdx;
    tile[threadIdx.x][threadIdx.y] = accVec[(y + offset) % N];
    __syncthreads();

    x = blockx + threadIdx.x;
    y = blocky + threadIdx.y;

    resMat[y * N + x] += tile[threadIdx.y][threadIdx.x];
}

__global__ void makeImage(
    complex* srcMat, int tupleCount)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    srcMat[idx] /= tupleCount;
}
}