/*
Copyright (c) Edgeless Systems GmbH

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, version 3 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

This file incorporates work covered by the following copyright and
permission notice:

Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud/metadata"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/driver/internal"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/mounter"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

func TestNewNodeService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetadataService := metadata.NewMockMetadataService(ctrl)
	mockMounter := mounter.NewMockMounter(ctrl)
	mockKubernetesClient := NewMockKubernetesClient(ctrl)

	os.Setenv("AWS_REGION", "us-west-2")
	defer os.Unsetenv("AWS_REGION")

	options := &Options{}

	nodeService := NewNodeService(options, mockMetadataService, mockMounter, mockKubernetesClient)

	if nodeService == nil {
		t.Fatal("Expected NewNodeService to return a non-nil NodeService")
	}

	if nodeService.metadata != mockMetadataService {
		t.Error("Expected NodeService.metadata to be set to the mock MetadataService")
	}

	if nodeService.mounter != mockMounter {
		t.Error("Expected NodeService.mounter to be set to the mock Mounter")
	}

	if nodeService.inFlight == nil {
		t.Error("Expected NodeService.inFlight to be initialized")
	}

	if nodeService.options != options {
		t.Error("Expected NodeService.options to be set to the provided options")
	}
}

func TestNodeStageVolume(t *testing.T) {

	var (
		targetPath     = "/dev/mapper"
		devicePath     = "/dev/fake"
		nvmeDevicePath = "/dev/nvmefake1n1"
		deviceFileInfo = fs.FileInfo(&fakeFileInfo{devicePath, os.ModeDevice})
		//deviceSymlinkFileInfo = fs.FileInfo(&fakeFileInfo{nvmeDevicePath, os.ModeSymlink})
		stdVolCap = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: FSTypeExt4,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
		stdVolContext           = map[string]string{VolumeAttributePartition: "1"}
		devicePathWithPartition = devicePath + "1"
		// With few exceptions, all "success" non-block cases have roughly the same
		// expected calls and only care about testing the FormatAndMountSensitiveWithFormatOptions call. The
		// exceptions should not call this, instead they should define expectMock
		// from scratch.
		successExpectMock = func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
			mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(false, nil)
			mockMounter.EXPECT().MakeDir(targetPath).Return(nil)
			mockMounter.EXPECT().GetDeviceNameFromMount(targetPath).Return("", 1, nil)
			mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil)
			mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)
			mockMounter.EXPECT().NeedResize(gomock.Eq(devicePath), gomock.Eq(targetPath)).Return(false, nil)
		}
	)
	testCases := []struct {
		name         string
		req          *csi.NodeStageVolumeRequest
		mounterMock  func(ctrl *gomock.Controller) *mounter.MockMounter
		metadataMock func(ctrl *gomock.Controller) *metadata.MockMetadataService
		expectedErr  error
		inflight     bool
	}{
		{
			name: "success",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{DevicePathKey: "/dev/xvdba"},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(true, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Any()).Return("", 1, nil)
				m.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().NeedResize(gomock.Any(), gomock.Any()).Return(false, nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedErr: nil,
		},
		{
			name: "missing_volume_id",
			req: &csi.NodeStageVolumeRequest{
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			name: "missing_staging_target",
			req: &csi.NodeStageVolumeRequest{
				VolumeId: "vol-test",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "Staging target not provided"),
		},
		{
			name: "missing_volume_capability",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "Volume capability not provided"),
		},
		{
			name: "invalid_volume_attribute",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeContext: map[string]string{
					VolumeAttributePartition: "invalid-partition",
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "Volume Attribute is not valid"),
		},
		{
			name: "unsupported_volume_capability",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_UNKNOWN,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "Volume capability not supported"),
		},
		{
			name: "block_volume",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  nil,
		},
		{
			name: "success with mount options",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							MountFlags: []string{"dirsync", "noexec"},
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeId: volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(FSTypeExt4), gomock.Eq([]string{"dirsync", "noexec"}), gomock.Nil(), gomock.Len(0))
			},
		},
		{
			name: "success fsType ext3",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: FSTypeExt3,
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeId: volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(FSTypeExt3), gomock.Any(), gomock.Nil(), gomock.Len(0))
			},
		},
		{
			name: "success mount with default fsType ext4",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeId: volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(FSTypeExt4), gomock.Any(), gomock.Nil(), gomock.Len(0))
			},
		},
		{
			name: "success device already mounted at target",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil)
				mockMounter.EXPECT().GetDeviceNameFromMount(targetPath).Return(devicePath, 1, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Nil(), gomock.Len(0)).Times(0)
			},
		},
		{
			name: "success nvme device already mounted at target",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil)

				// If the device is nvme GetDeviceNameFromMount should return the
				// canonical device path
				mockMounter.EXPECT().GetDeviceNameFromMount(targetPath).Return(nvmeDevicePath, 1, nil)

				// The publish context device path may not exist but the driver should
				// find the canonical device path (see TestFindDevicePath), compare it
				// to the one returned by GetDeviceNameFromMount, and then skip
				// FormatAndMountSensitiveWithFormatOptions
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(nvmeName)).Return(symlinkFileInfo, nil)
				mockDeviceIdentifier.EXPECT().EvalSymlinks(gomock.Eq(symlinkFileInfo.Name())).Return(nvmeDevicePath, nil)

				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Nil(), gomock.Len(0)).Times(0)
			},
		},
		{
			name: "success with partition",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeContext:     stdVolContext,
				VolumeId:          volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(false, nil)
				mockMounter.EXPECT().MakeDir(targetPath).Return(nil)
				mockMounter.EXPECT().GetDeviceNameFromMount(targetPath).Return("", 1, nil)
				mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				// The device path argument should be canonicalized to contain the
				// partition
				mockMounter.EXPECT().NeedResize(gomock.Eq(devicePathWithPartition), gomock.Eq(targetPath)).Return(false, nil)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePathWithPartition), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Len(0))
			},
		},
		{
			name: "success with invalid partition config, will ignore partition",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeContext:     map[string]string{VolumeAttributePartition: "0"},
				VolumeId:          volumeID,
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Len(0))
			},
		},
		{
			name: "success with block size",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{BlockSizeKey: "1024"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-b", "1024"}))
			},
		},
		{
			name: "success with inode size in ext4",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{InodeSizeKey: "256"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-I", "256"}))
			},
		},
		{
			name: "success with inode size in xfs",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: FSTypeXfs,
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeId:      volumeID,
				VolumeContext: map[string]string{InodeSizeKey: "256"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(FSTypeXfs), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-i", "size=256"}))
			},
		},
		{
			name: "success with bytes-per-inode",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{BytesPerInodeKey: "8192"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-i", "8192"}))
			},
		},
		{
			name: "success with number-of-inodes",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{NumberOfInodesKey: "13107200"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-N", "13107200"}))
			},
		},
		{
			name: "success with bigalloc feature flag enabled in ext4",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{Ext4BigAllocKey: "true"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-O", "bigalloc"}))
			},
		},
		{
			name: "success with custom cluster size and bigalloc feature flag enabled in ext4",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
				VolumeId:          volumeID,
				VolumeContext:     map[string]string{Ext4BigAllocKey: "true", Ext4ClusterSizeKey: "16384"},
			},
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				successExpectMock(mockMounter, mockDeviceIdentifier)
				mockMounter.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Any(), gomock.Nil(), gomock.Eq([]string{"-O", "bigalloc", "-C", "16384"}))
			},
		},
		{
			name: "fail no VolumeId",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability:  stdVolCap,
			},
			expectedCode: codes.InvalidArgument,
		},
		{
			name: "fail no mount",
			request: &csi.NodeStageVolumeRequest{
				PublishContext:    map[string]string{DevicePathKey: devicePath},
				StagingTargetPath: targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Error(codes.InvalidArgument, "NodeStageVolume: mount is nil within volume capability"),
		},
		{
			name: "default_fstype",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{DevicePathKey: "/dev/xvdba"},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(false, nil)
				m.EXPECT().MakeDir(gomock.Any()).Return(nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Any()).Return("", 0, nil)
				m.EXPECT().FormatAndMountSensitiveWithFormatOptions(gomock.Any(), gomock.Any(), defaultFsType, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				m.EXPECT().NeedResize(gomock.Any(), gomock.Any()).Return(false, nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedErr: nil,
		},
		{
			name: "invalid_fstype",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "invalid",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			mounterMock:  nil,
			metadataMock: nil,
			expectedErr:  status.Errorf(codes.InvalidArgument, "NodeStageVolume: invalid fstype invalid"),
		},
		{
			name: "invalid_block_size",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeContext: map[string]string{
					BlockSizeKey: "-",
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				return nil
			},
			expectedErr: status.Error(codes.InvalidArgument, "Invalid blocksize (aborting!): <nil>"),
		},
		{
			name: "invalid_inode_size",
			req: &csi.NodeStageVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType: "ext4",
						},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				VolumeContext: map[string]string{
					InodeSizeKey: "-",
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				return m
			},
			expectedCode: codes.Aborted,
			expectMock: func(mockMounter MockMounter, mockDeviceIdentifier MockDeviceIdentifier) {
				mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl)
			}

			awsDriver := &nodeService{
				metadata:         mockMetadata,
				mounter:          mockMounter,
				deviceIdentifier: mockDeviceIdentifier,
				inFlight:         inFlight,
				driverOptions: &DriverOptions{
					cm: &stubCryptMapper{},
				},
			}

			driver := &NodeService{
				metadata: metadata,
				mounter:  mounter,
				inFlight: internal.NewInFlight(),
			}

			if tc.inflight {
				driver.inFlight.Insert("vol-test")
			}

			_, err := driver.NodeStageVolume(context.Background(), tc.req)
			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr, err)
			}
		})
	}
}

func TestNodeUnstageVolume(t *testing.T) {
	targetPath := "/dev/mapper"
	devicePath := "/dev/fake"

	testCases := []struct {
		name         string
		expectedErr  error
		expectedVal  int64
		options      *Options
		metadataMock func(ctrl *gomock.Controller) *metadata.MockMetadataService
	}{
		{
			name: "success normal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().GetDeviceNameFromMount(gomock.Eq(targetPath)).Return(devicePath, 1, nil)
				mockMounter.EXPECT().Unstage(gomock.Eq(targetPath)).Return(nil)

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success no device mounted at target",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().GetDeviceNameFromMount(gomock.Eq(targetPath)).Return(devicePath, 0, nil)

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
					VolumeId:          volumeID,
				}
				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success device mounted at multiple targets",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().GetDeviceNameFromMount(gomock.Eq(targetPath)).Return(devicePath, 2, nil)
				mockMounter.EXPECT().Unstage(gomock.Eq(targetPath)).Return(nil)

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "fail no VolumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
				}

				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)
			},
		},
		{
			name: "fail no StagingTargetPath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnstageVolumeRequest{
					VolumeId: volumeID,
				}
				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)
			},
		},
		{
			name: "fail GetDeviceName returns error",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().GetDeviceNameFromMount(gomock.Eq(targetPath)).Return("", 0, errors.New("GetDeviceName faield"))

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
		{
			name: "fail another operation in-flight on given volumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnstageVolumeRequest{
					StagingTargetPath: targetPath,
					VolumeId:          volumeID,
				}

				awsDriver.inFlight.Insert(volumeID)
				_, err := awsDriver.NodeUnstageVolume(context.TODO(), req)
				expectErr(t, err, codes.Aborted)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter

			var metadata *metadata.MockMetadataService
			if tc.metadataMock != nil {
				metadata = tc.metadataMock(ctrl)
			}

			driver := &NodeService{
				mounter:  mounter,
				inFlight: internal.NewInFlight(),
				options:  tc.options,
				metadata: metadata,
			}

			value := driver.getVolumesLimit()
			if value != tc.expectedVal {
				t.Fatalf("Expected value %v but got %v", tc.expectedVal, value)
			}
		})
	}
}

func TestNodePublishVolume(t *testing.T) {
	targetPath := "/dev/mapper"
	stagingTargetPath := "/test/staging/path"
	devicePath := "/dev/fake"
	deviceFileInfo := fs.FileInfo(&fakeFileInfo{devicePath, os.ModeDevice})
	stdVolCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
	stdVolContext := map[string]string{"partition": "1"}
	devicePathWithPartition := devicePath + "1"
	testCases := []struct {
		name         string
		req          *csi.NodePublishVolumeRequest
		mounterMock  func(ctrl *gomock.Controller) *mounter.MockMounter
		metadataMock func(ctrl *gomock.Controller) *metadata.MockMetadataService
		expectedErr  error
		inflight     bool
	}{
		{
			name: "success normal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(stagingTargetPath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Eq([]string{"bind"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success filesystem mounted already",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(false, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success filesystem mountpoint error",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(true, nil),
				)
				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, errors.New("Internal system error"))

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
		{
			name: "success filesystem corrupted mountpoint error",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(true, errors.New("CorruptedMntError"))
				mockMounter.EXPECT().IsCorruptedMnt(gomock.Eq(errors.New("CorruptedMntError"))).Return(true)

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, errors.New("internal system error"))
				mockMounter.EXPECT().Unpublish(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(stagingTargetPath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Eq([]string{"bind"})).Return(nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success fstype xfs",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(stagingTargetPath), gomock.Eq(targetPath), gomock.Eq(FSTypeXfs), gomock.Eq([]string{"bind", "nouuid"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								FsType: FSTypeXfs,
							},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success readonly",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(stagingTargetPath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Eq([]string{"bind", "ro"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					Readonly:          true,
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success mount options",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				mockMounter.EXPECT().MakeDir(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(stagingTargetPath), gomock.Eq(targetPath), gomock.Eq(defaultFsType), gomock.Eq([]string{"bind", "test-flag"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{
								// this request will call mount with the bind option,
								// adding "bind" here we test that we don't add the
								// same option twice. "test-flag" is a canary to check
								// that the driver calls mount with that flag
								MountFlags: []string{"bind", "test-flag"},
							},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success normal [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
				)
				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(""), gomock.Eq([]string{"bind"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success mounted already [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
				)
				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(false, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success mountpoint error [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(true, nil),
				)

				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, errors.New("Internal System Error"))

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
		{
			name: "success corrupted mountpoint error [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq(targetPath)).Return(true, errors.New("CorruptedMntError")),
				)

				mockMounter.EXPECT().IsCorruptedMnt(errors.New("CorruptedMntError")).Return(true)

				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().Unpublish(gomock.Eq(targetPath)).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, errors.New("Internal System Error"))
				mockMounter.EXPECT().Mount(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Any(), gomock.Any()).Return(nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success normal with partition [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
				)
				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(devicePathWithPartition), gomock.Eq(targetPath), gomock.Eq(""), gomock.Eq([]string{"bind"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeContext: stdVolContext,
					VolumeId:      volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "success normal with invalid partition config, will ignore the config [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				gomock.InOrder(
					mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(true, nil),
					mockMounter.EXPECT().PathExists(gomock.Eq("/test")).Return(false, nil),
				)
				mockMounter.EXPECT().MakeDir(gomock.Eq("/test")).Return(nil)
				mockMounter.EXPECT().MakeFile(targetPath).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Eq(devicePath), gomock.Eq(targetPath), gomock.Eq(""), gomock.Eq([]string{"bind"})).Return(nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Eq(targetPath)).Return(true, nil)
				mockDeviceIdentifier.EXPECT().Lstat(gomock.Eq(devicePath)).Return(deviceFileInfo, nil)

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeContext: map[string]string{VolumeAttributePartition: "0"},
					VolumeId:      volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "Fail invalid volumeContext config [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeContext: map[string]string{VolumeAttributePartition: "partition1"},
					VolumeId:      volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)
			},
		},
		{
			name: "fail no device path [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail to find deivce path [raw block]",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
					VolumeId: volumeID,
				}

				mockMounter.EXPECT().PathExists(gomock.Eq(devicePath)).Return(false, errors.New("findDevicePath failed"))

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Internal)

			},
		},
		{
			name: "fail no VolumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeCapability:  stdVolCap,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail no StagingTargetPath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:   map[string]string{DevicePathKey: devicePath},
					TargetPath:       targetPath,
					VolumeCapability: stdVolCap,
					VolumeId:         volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail no TargetPath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					VolumeCapability:  stdVolCap,
					VolumeId:          volumeID,
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail no VolumeCapability",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: devicePath},
					StagingTargetPath: stagingTargetPath,
					TargetPath:        targetPath,
					VolumeId:          volumeID,
				}
				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail invalid VolumeCapability",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: "/test/staging/path",
					TargetPath:        "/test/target/path",
					VolumeId:          volumeID,
					VolumeCapability: &csi.VolumeCapability{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_UNKNOWN,
						},
					},
				}

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)

			},
		},
		{
			name: "fail another operation in-flight on given volumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodePublishVolumeRequest{
					PublishContext:    map[string]string{DevicePathKey: "/dev/fake"},
					StagingTargetPath: "/test/staging/path",
					TargetPath:        "/test/target/path",
					VolumeId:          volumeID,
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{
							Block: &csi.VolumeCapability_BlockVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
						},
					},
				}
				awsDriver.inFlight.Insert(volumeID)

				_, err := awsDriver.NodePublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Aborted)

			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}
func TestNodeExpandVolume(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockMetadata := cloud.NewMockMetadataService(mockCtl)
	mockMounter := NewMockMounter(mockCtl)
	mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

	awsDriver := &nodeService{
		metadata:         mockMetadata,
		mounter:          mockMounter,
		deviceIdentifier: mockDeviceIdentifier,
		inFlight:         internal.NewInFlight(),
		driverOptions: &DriverOptions{
			cm: &stubCryptMapper{},
		},
	}

	tests := []struct {
		name               string
		request            csi.NodeExpandVolumeRequest
		expectResponseCode codes.Code
	}{
		{
			name:               "fail missing volumeId",
			request:            csi.NodeExpandVolumeRequest{},
			expectResponseCode: codes.InvalidArgument,
		},
		{
			name: "fail missing volumePath",
			request: csi.NodeExpandVolumeRequest{
				StagingTargetPath: "/testDevice/Path",
				VolumeId:          "test-volume-id",
			},
			expectResponseCode: codes.InvalidArgument,
		},
		{
			name: "fail volume path not exist",
			request: csi.NodeExpandVolumeRequest{
				VolumePath: "./test",
				VolumeId:   "test-volume-id",
			},
			expectResponseCode: codes.Internal,
		},
		{
			name: "Fail validate VolumeCapability",
			request: csi.NodeExpandVolumeRequest{
				VolumePath: "./test",
				VolumeId:   "test-volume-id",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)

				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(true, nil)
				m.EXPECT().MakeFile(gomock.Any()).Return(nil)
				m.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				m.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
		},
		{
			name: "success_fs",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PreparePublishTarget(gomock.Any()).Return(nil)
				m.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				m.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return m
			},
		},
		{
			name: "volume_id_not_provided",
			req: &csi.NodePublishVolumeRequest{
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			name: "staging_target_path_not_provided",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:   "vol-test",
				TargetPath: "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "Staging target not provided"),
		},
		{
			name: "target_path_not_provided",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "Target path not provided"),
		},
		{
			name: "capability_not_provided",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume capability not provided"),
		},
		{
			name: "success_block_device",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			inflight:    true,
			expectedErr: status.Errorf(codes.Aborted, VolumeOperationAlreadyExists, "vol-test"),
		},
		{
			name: "success_block_device",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_Mode(csi.ControllerServiceCapability_RPC_UNKNOWN),
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			expectedErr: status.Errorf(codes.InvalidArgument, "Volume capability not supported"),
		},
		{
			name: "read_only_enabled",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
				Readonly: true,
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)

				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(true, nil)
				m.EXPECT().MakeFile(gomock.Any()).Return(nil)
				m.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				m.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
		},
		{
			name: "nodePublishVolumeForBlock_error",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			expectedErr: status.Errorf(codes.InvalidArgument, "Device path not provided"),
		},
		{
			name: "nodePublishVolumeForBlock_invalid_volume_attribute",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
				VolumeContext: map[string]string{
					VolumeAttributePartition: "invalid-partition",
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume Attribute is invalid"),
		},
		{
			name: "nodePublishVolumeForBlock_invalid_partition",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
				VolumeContext: map[string]string{
					VolumeAttributePartition: "0",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)

				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(true, nil)
				m.EXPECT().MakeFile(gomock.Any()).Return(nil)
				m.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				m.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
		},
		{
			name: "nodePublishVolumeForBlock_valid_partition",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
				VolumeContext: map[string]string{
					VolumeAttributePartition: "1",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)

				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/dev/xvdba", nil)
				m.EXPECT().PathExists(gomock.Any()).Return(true, nil)
				m.EXPECT().MakeFile(gomock.Any()).Return(nil)
				m.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				m.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
		},
		{
			name: "nodePublishVolumeForBlock_device_path_failure",
			req: &csi.NodePublishVolumeRequest{
				VolumeId:          "vol-test",
				StagingTargetPath: "/staging/path",
				TargetPath:        "/target/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
				PublishContext: map[string]string{
					DevicePathKey: "/dev/xvdba",
				},
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)

				m.EXPECT().FindDevicePath(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", errors.New("device path error"))
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedErr: status.Error(codes.Internal, "Failed to find device path /dev/xvdba. device path error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl)
			}

			var metadata *metadata.MockMetadataService
			if tc.metadataMock != nil {
				metadata = tc.metadataMock(ctrl)
			}

			driver := &NodeService{
				metadata: metadata,
				mounter:  mounter,
				inFlight: internal.NewInFlight(),
			}

			if tc.inflight {
				driver.inFlight.Insert("vol-test")
			}

			_, err := driver.NodePublishVolume(context.Background(), tc.req)
			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr, err)
			}
		})
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	targetPath := "/dev/mapper"

	testCases := []struct {
		name        string
		req         *csi.NodeUnstageVolumeRequest
		mounterMock func(ctrl *gomock.Controller) *mounter.MockMounter
		expectedErr error
		inflight    bool
	}{
		{
			name: "success normal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnpublishVolumeRequest{
					TargetPath: targetPath,
					VolumeId:   volumeID,
				}

				mockMounter.EXPECT().Unpublish(gomock.Eq(targetPath)).Return(nil)
				_, err := awsDriver.NodeUnpublishVolume(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "fail no VolumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnpublishVolumeRequest{
					TargetPath: targetPath,
				}

				_, err := awsDriver.NodeUnpublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)
			},
		},
		{
			name: "fail no TargetPath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnpublishVolumeRequest{
					VolumeId: volumeID,
				}

				_, err := awsDriver.NodeUnpublishVolume(context.TODO(), req)
				expectErr(t, err, codes.InvalidArgument)
			},
		},
		{
			name: "fail error on unpublish",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnpublishVolumeRequest{
					TargetPath: targetPath,
					VolumeId:   volumeID,
				}

				mockMounter.EXPECT().Unpublish(gomock.Eq(targetPath)).Return(errors.New("test Unpublish error"))
				_, err := awsDriver.NodeUnpublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
		{
			name: "fail another operation in-flight on given volumeId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

				awsDriver := &nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeUnpublishVolumeRequest{
					TargetPath: targetPath,
					VolumeId:   volumeID,
				}

				awsDriver.inFlight.Insert(volumeID)
				_, err := awsDriver.NodeUnpublishVolume(context.TODO(), req)
				expectErr(t, err, codes.Aborted)
			},
			expectedErr: status.Error(codes.Aborted, "An operation with the given volume=\"vol-test\" is already in progress"),
			inflight:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl)
			}

			driver := &NodeService{
				mounter:  mounter,
				inFlight: internal.NewInFlight(),
			}

			if tc.inflight {
				driver.inFlight.Insert("vol-test")
			}

			_, err := driver.NodeUnstageVolume(context.Background(), tc.req)
			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr, err)
			}
		})
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "success normal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)
				VolumePath := "./test"
				err := os.MkdirAll(VolumePath, 0644)
				if err != nil {
					t.Fatalf("fail to create dir: %v", err)
				}
				defer os.RemoveAll(VolumePath)

				mockMounter.EXPECT().PathExists(VolumePath).Return(true, nil)

				awsDriver := nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeGetVolumeStatsRequest{
					VolumeId:   volumeID,
					VolumePath: VolumePath,
				}
				_, err = awsDriver.NodeGetVolumeStats(context.TODO(), req)
				if err != nil {
					t.Fatalf("Expect no error but got: %v", err)
				}
			},
		},
		{
			name: "fail path not exist",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)
				VolumePath := "/test"

				mockMounter.EXPECT().PathExists(VolumePath).Return(false, nil)

				awsDriver := nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeGetVolumeStatsRequest{
					VolumeId:   volumeID,
					VolumePath: VolumePath,
				}
				_, err := awsDriver.NodeGetVolumeStats(context.TODO(), req)
				expectErr(t, err, codes.NotFound)
			},
		},
		{
			name: "fail can't determine block device",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)
				VolumePath := "/test"

				mockMounter.EXPECT().PathExists(VolumePath).Return(true, nil)

				awsDriver := nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeGetVolumeStatsRequest{
					VolumeId:   volumeID,
					VolumePath: VolumePath,
				}
				_, err := awsDriver.NodeGetVolumeStats(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
		{
			name: "fail error calling existsPath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockMetadata := cloud.NewMockMetadataService(mockCtl)
				mockMounter := NewMockMounter(mockCtl)
				mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)
				VolumePath := "/test"

				mockMounter.EXPECT().PathExists(VolumePath).Return(false, errors.New("get existsPath call fail"))

				awsDriver := nodeService{
					metadata:         mockMetadata,
					mounter:          mockMounter,
					deviceIdentifier: mockDeviceIdentifier,
					inFlight:         internal.NewInFlight(),
					driverOptions: &DriverOptions{
						cm: &stubCryptMapper{},
					},
				}

				req := &csi.NodeGetVolumeStatsRequest{
					VolumeId:   volumeID,
					VolumePath: VolumePath,
				}
				_, err := awsDriver.NodeGetVolumeStats(context.TODO(), req)
				expectErr(t, err, codes.Internal)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}

}

func TestNodeGetCapabilities(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockMetadata := cloud.NewMockMetadataService(mockCtl)
	mockMounter := NewMockMounter(mockCtl)
	mockDeviceIdentifier := NewMockDeviceIdentifier(mockCtl)

	awsDriver := nodeService{
		metadata:         mockMetadata,
		mounter:          mockMounter,
		deviceIdentifier: mockDeviceIdentifier,
		inFlight:         internal.NewInFlight(),
		driverOptions: &DriverOptions{
			cm: &stubCryptMapper{},
		},
	}

	caps := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				},
			},
		},
	}

	driver := &NodeService{}

	resp, err := driver.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(resp.GetCapabilities()) != len(expectedCaps) {
		t.Fatalf("Expected %d capabilities, but got %d", len(expectedCaps), len(resp.GetCapabilities()))
	}

	for i, cap := range resp.GetCapabilities() {
		if cap.GetRpc().GetType() != expectedCaps[i].GetRpc().GetType() {
			t.Fatalf("Expected capability %v, but got %v", expectedCaps[i].GetRpc().GetType(), cap.GetRpc().GetType())
		}
	}
}

func TestNodeGetInfo(t *testing.T) {
	testCases := []struct {
		name         string
		metadataMock func(ctrl *gomock.Controller) *metadata.MockMetadataService
		expectedResp *csi.NodeGetInfoResponse
	}{
		{
			name: "without_outpost_arn",
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetInstanceID().Return("i-1234567890abcdef0")
				m.EXPECT().GetAvailabilityZone().Return("us-west-2a")
				m.EXPECT().GetOutpostArn().Return(arn.ARN{})
				return m
			},
			expectedResp: &csi.NodeGetInfoResponse{
				NodeId: "i-1234567890abcdef0",
				AccessibleTopology: &csi.Topology{
					Segments: map[string]string{
						ZoneTopologyKey:          "us-west-2a",
						WellKnownZoneTopologyKey: "us-west-2a",
						OSTopologyKey:            runtime.GOOS,
					},
				},
			},
		},
		{
			name: "with_outpost_arn",
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetInstanceID().Return("i-1234567890abcdef0")
				m.EXPECT().GetAvailabilityZone().Return("us-west-2a")
				m.EXPECT().GetOutpostArn().Return(arn.ARN{
					Partition: "aws",
					Service:   "outposts",
					Region:    "us-west-2",
					AccountID: "123456789012",
					Resource:  "op-1234567890abcdef0",
				})
				return m
			},
			expectedResp: &csi.NodeGetInfoResponse{
				NodeId: "i-1234567890abcdef0",
				AccessibleTopology: &csi.Topology{
					Segments: map[string]string{
						ZoneTopologyKey:          "us-west-2a",
						WellKnownZoneTopologyKey: "us-west-2a",
						OSTopologyKey:            runtime.GOOS,
						AwsRegionKey:             "us-west-2",
						AwsPartitionKey:          "aws",
						AwsAccountIDKey:          "123456789012",
						AwsOutpostIDKey:          "op-1234567890abcdef0",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driverOptions := &DriverOptions{
				volumeAttachLimit: tc.volumeAttachLimit,
				cm:                &stubCryptMapper{},
			}

			resp, err := driver.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !reflect.DeepEqual(resp, tc.expectedResp) {
				t.Fatalf("Expected response %+v, but got %+v", tc.expectedResp, resp)
			}
		})
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	testCases := []struct {
		name        string
		req         *csi.NodeUnpublishVolumeRequest
		mounterMock func(ctrl *gomock.Controller) *mounter.MockMounter
		expectedErr error
		inflight    bool
	}{
		{
			name: "success",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "vol-test",
				TargetPath: "/target/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().Unpublish(gomock.Eq("/target/path")).Return(nil)
				return m
			},
		},
		{
			name: "missing_volume_id",
			req: &csi.NodeUnpublishVolumeRequest{
				TargetPath: "/target/path",
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			name: "missing_target_path",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId: "vol-test",
			},
			expectedErr: status.Error(codes.InvalidArgument, "Target path not provided"),
		},
		{
			name: "unpublish_failed",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "vol-test",
				TargetPath: "/target/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().Unpublish(gomock.Eq("/target/path")).Return(errors.New("unpublish failed"))
				return m
			},
			expectedErr: status.Errorf(codes.Internal, "Could not unmount %q: %v", "/target/path", errors.New("unpublish failed")),
		},
		{
			name: "operation_already_exists",
			req: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "vol-test",
				TargetPath: "/target/path",
			},
			expectedErr: status.Error(codes.Aborted, "An operation with the given volume=\"vol-test\" is already in progress"),
			inflight:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl)
			}

			driver := &NodeService{
				mounter:  mounter,
				inFlight: internal.NewInFlight(),
			}

			if tc.inflight {
				driver.inFlight.Insert("vol-test")
			}

			_, err := driver.NodeUnpublishVolume(context.Background(), tc.req)
			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr, err)
			}
		})
	}
}

func TestNodeExpandVolume(t *testing.T) {
	testCases := []struct {
		name         string
		req          *csi.NodeExpandVolumeRequest
		mounterMock  func(ctrl *gomock.Controller) *mounter.MockMounter
		metadataMock func(ctrl *gomock.Controller) *metadata.MockMetadataService
		expectedResp *csi.NodeExpandVolumeResponse
		expectedErr  error
	}{
		{
			name: "success",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Eq("/volume/path")).Return("device-name", 1, nil)
				m.EXPECT().FindDevicePath(gomock.Eq("device-name"), gomock.Eq("vol-test"), gomock.Eq(""), gomock.Eq("us-west-2")).Return("/dev/xvdba", nil)
				m.EXPECT().Resize(gomock.Eq("/dev/xvdba"), gomock.Eq("/volume/path")).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(gomock.Eq("/dev/xvdba")).Return(int64(1000), nil)
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedResp: &csi.NodeExpandVolumeResponse{CapacityBytes: int64(1000)},
		},
		{
			name: "missing_volume_id",
			req: &csi.NodeExpandVolumeRequest{
				VolumePath: "/volume/path",
			},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			name: "missing_volume_path",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId: "vol-test",
			},
			expectedErr: status.Error(codes.InvalidArgument, "volume path must be provided"),
		},
		{
			name: "invalid_volume_capability",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_UNKNOWN,
					},
				},
			},
			expectedErr: status.Error(codes.InvalidArgument, "VolumeCapability is invalid: block:<> access_mode:<> "),
		},
		{
			name: "block_device",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			expectedResp: &csi.NodeExpandVolumeResponse{},
		},
		{
			name: "is_block_device_error",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, errors.New("failed to determine if block device"))
				return m
			},
			expectedErr: status.Error(codes.Internal, "failed to determine if volumePath [/volume/path] is a block device: failed to determine if block device"),
		},
		{
			name: "get_block_size_bytes_error",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(gomock.Eq("/volume/path")).Return(int64(0), errors.New("failed to get block size"))
				return m
			},
			expectedErr: status.Error(codes.Internal, "failed to get block capacity on path /volume/path: failed to get block size"),
		},
		{
			name: "block_device_success",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(gomock.Eq("/volume/path")).Return(int64(1000), nil)
				return m
			},
			expectedResp: &csi.NodeExpandVolumeResponse{CapacityBytes: int64(1000)},
		},
		{
			name: "get_device_name_error",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Eq("/volume/path")).Return("", 0, errors.New("failed to get device name"))
				return m
			},
			metadataMock: nil,
			expectedResp: nil,
			expectedErr:  status.Error(codes.Internal, "failed to get device name from mount /volume/path: failed to get device name"),
		},
		{
			name: "find_device_path_error",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Eq("/volume/path")).Return("device-name", 1, nil)
				m.EXPECT().FindDevicePath(gomock.Eq("device-name"), gomock.Eq("vol-test"), gomock.Eq(""), gomock.Eq("us-west-2")).Return("", errors.New("failed to find device path"))
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedResp: nil,
			expectedErr:  status.Error(codes.Internal, "failed to find device path for device name device-name for mount /volume/path: failed to find device path"),
		},
		{
			name: "resize_error",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Eq("/volume/path")).Return("device-name", 1, nil)
				m.EXPECT().FindDevicePath(gomock.Eq("device-name"), gomock.Eq("vol-test"), gomock.Eq(""), gomock.Eq("us-west-2")).Return("/dev/xvdba", nil)
				m.EXPECT().Resize(gomock.Eq("/dev/xvdba"), gomock.Eq("/volume/path")).Return(false, errors.New("failed to resize volume"))
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedResp: nil,
			expectedErr:  status.Error(codes.Internal, "Could not resize volume \"vol-test\" (\"/dev/xvdba\"): failed to resize volume"),
		},
		{
			name: "get_block_size_bytes_error_after_resize",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId:   "vol-test",
				VolumePath: "/volume/path",
			},
			mounterMock: func(ctrl *gomock.Controller) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().IsBlockDevice(gomock.Eq("/volume/path")).Return(false, nil)
				m.EXPECT().GetDeviceNameFromMount(gomock.Eq("/volume/path")).Return("device-name", 1, nil)
				m.EXPECT().FindDevicePath(gomock.Eq("device-name"), gomock.Eq("vol-test"), gomock.Eq(""), gomock.Eq("us-west-2")).Return("/dev/xvdba", nil)
				m.EXPECT().Resize(gomock.Eq("/dev/xvdba"), gomock.Eq("/volume/path")).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(gomock.Eq("/dev/xvdba")).Return(int64(0), errors.New("failed to get block size"))
				return m
			},
			metadataMock: func(ctrl *gomock.Controller) *metadata.MockMetadataService {
				m := metadata.NewMockMetadataService(ctrl)
				m.EXPECT().GetRegion().Return("us-west-2")
				return m
			},
			expectedResp: nil,
			expectedErr:  status.Error(codes.Internal, "failed to get block capacity on path /volume/path: failed to get block size"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl)
			}

			var metadata *metadata.MockMetadataService
			if tc.metadataMock != nil {
				metadata = tc.metadataMock(ctrl)
			}

			driver := &NodeService{
				mounter:  mounter,
				metadata: metadata,
			}

			resp, err := driver.NodeExpandVolume(context.Background(), tc.req)
			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr, err)
			}

			if !reflect.DeepEqual(resp, tc.expectedResp) {
				t.Fatalf("Expected response '%v' but got '%v'", tc.expectedResp, resp)
			}
		})
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	testCases := []struct {
		name           string
		validVolId     bool
		validPath      bool
		metricsStatErr bool
		mounterMock    func(mockCtl *gomock.Controller, dir string) *mounter.MockMounter
		expectedErr    func(dir string) error
	}{
		{
			name:       "success normal",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(true, nil)
				m.EXPECT().IsBlockDevice(gomock.Eq(dir)).Return(false, nil)
				return m
			},
			expectedErr: func(dir string) error {
				return nil
			},
		},
		{
			name:       "invalid_volume_id",
			validVolId: false,
			expectedErr: func(dir string) error {
				return status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty")
			},
		},
		{
			name:       "invalid_volume_path",
			validVolId: true,
			validPath:  false,
			expectedErr: func(dir string) error {
				return status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty")
			},
		},
		{
			name:       "path_exists_error",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(false, errors.New("path exists error"))
				return m
			},
			expectedErr: func(dir string) error {
				return status.Errorf(codes.Internal, "unknown error when stat on %s: path exists error", dir)
			},
		},
		{
			name:       "path_does_not_exist",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(false, nil)
				return m
			},
			expectedErr: func(dir string) error {
				return status.Errorf(codes.NotFound, "path %s does not exist", dir)
			},
		},
		{
			name:       "is_block_device_error",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(true, nil)
				m.EXPECT().IsBlockDevice(gomock.Eq(dir)).Return(false, errors.New("is block device error"))
				return m
			},
			expectedErr: func(dir string) error {
				return status.Errorf(codes.Internal, "failed to determine whether %s is block device: is block device error", dir)
			},
		},
		{
			name:       "get_block_size_bytes_error",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(true, nil)
				m.EXPECT().IsBlockDevice(gomock.Eq(dir)).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(dir).Return(int64(0), errors.New("get block size bytes error"))
				return m
			},
			expectedErr: func(dir string) error {
				return status.Errorf(codes.Internal, "failed to get block capacity on path %s: %v", dir, "get block size bytes error")
			},
		},
		{
			name:       "success block device",
			validVolId: true,
			validPath:  true,
			mounterMock: func(ctrl *gomock.Controller, dir string) *mounter.MockMounter {
				m := mounter.NewMockMounter(ctrl)
				m.EXPECT().PathExists(dir).Return(true, nil)
				m.EXPECT().IsBlockDevice(gomock.Eq(dir)).Return(true, nil)
				m.EXPECT().GetBlockSizeBytes(dir).Return(int64(1024), nil)
				return m
			},
			expectedErr: func(dir string) error {
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dir := t.TempDir()

			var mounter *mounter.MockMounter
			if tc.mounterMock != nil {
				mounter = tc.mounterMock(ctrl, dir)
			}

			var metadata *metadata.MockMetadataService
			driver := &NodeService{
				mounter:  mounter,
				metadata: metadata,
			}

			req := &csi.NodeGetVolumeStatsRequest{}
			if tc.validVolId {
				req.VolumeId = "vol-test"
			}
			if tc.validPath {
				req.VolumePath = dir
			}
			if tc.metricsStatErr {
				req.VolumePath = "fake-path"
			}

			_, err := driver.NodeGetVolumeStats(context.TODO(), req)

			if !reflect.DeepEqual(err, tc.expectedErr(dir)) {
				t.Fatalf("Expected error '%v' but got '%v'", tc.expectedErr(dir), err)
			}
		})
	}
}

func TestRemoveNotReadyTaint(t *testing.T) {
	nodeName := "test-node-123"
	testCases := []struct {
		name      string
		setup     func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error)
		expResult error
	}{
		{
			name: "failed to get node",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, _ := getNodeMock(mockCtl, nodeName, nil, fmt.Errorf("Failed to get node!"))

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: fmt.Errorf("Failed to get node!"),
		},
		{
			name: "no taints to remove",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, _ := getNodeMock(mockCtl, nodeName, &corev1.Node{}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				count := int32(1)

				mockCSINode := &v1.CSINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node-123",
					},
					Spec: v1.CSINodeSpec{
						Drivers: []v1.CSINodeDriver{
							{
								Name: DriverName,
								Allocatable: &v1.VolumeNodeResources{
									Count: &count,
								},
							},
						},
					},
				}

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq("test-node-123"), gomock.Any()).
					Return(mockCSINode, nil).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: nil,
		},
		{
			name: "failed to patch node",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, mockNode := getNodeMock(mockCtl, nodeName, &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key:    AgentNotReadyNodeTaintKey,
								Effect: corev1.TaintEffectNoExecute,
							},
						},
					},
				}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				count := int32(1)
				mockCSINode := &v1.CSINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.CSINodeSpec{
						Drivers: []v1.CSINodeDriver{
							{
								Name:   DriverName,
								NodeID: nodeName,
								Allocatable: &v1.VolumeNodeResources{
									Count: &count,
								},
							},
						},
					},
				}

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).
					Return(mockCSINode, nil).
					Times(1)

				mockNode.EXPECT().
					Patch(gomock.Any(), gomock.Eq(nodeName), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("Failed to patch node!")).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: fmt.Errorf("Failed to patch node!"),
		},
		{
			name: "success",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, mockNode := getNodeMock(mockCtl, nodeName, &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key:    AgentNotReadyNodeTaintKey,
								Effect: corev1.TaintEffectNoSchedule,
							},
						},
					},
				}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				count := int32(1)
				mockCSINode := &v1.CSINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.CSINodeSpec{
						Drivers: []v1.CSINodeDriver{
							{
								Name:   DriverName,
								NodeID: nodeName,
								Allocatable: &v1.VolumeNodeResources{
									Count: &count,
								},
							},
						},
					},
				}

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).
					Return(mockCSINode, nil).
					Times(1)

				mockNode.EXPECT().
					Patch(gomock.Any(), gomock.Eq(nodeName), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, nil).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: nil,
		},
		{
			name: "failed to get CSINode",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, _ := getNodeMock(mockCtl, nodeName, &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key:    AgentNotReadyNodeTaintKey,
								Effect: corev1.TaintEffectNoSchedule,
							},
						},
					},
				}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).
					Return(nil, fmt.Errorf("Failed to get CSINode")).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: fmt.Errorf("isAllocatableSet: failed to get CSINode for %s: Failed to get CSINode", nodeName),
		},
		{
			name: "allocatable value not set for driver on node",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, _ := getNodeMock(mockCtl, nodeName, &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key:    AgentNotReadyNodeTaintKey,
								Effect: corev1.TaintEffectNoSchedule,
							},
						},
					},
				}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				mockCSINode := &v1.CSINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.CSINodeSpec{
						Drivers: []v1.CSINodeDriver{
							{
								Name:   DriverName,
								NodeID: nodeName,
							},
						},
					},
				}

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).
					Return(mockCSINode, nil).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: fmt.Errorf("isAllocatableSet: allocatable value not set for driver on node %s", nodeName),
		},
		{
			name: "driver not found on node",
			setup: func(t *testing.T, mockCtl *gomock.Controller) func() (kubernetes.Interface, error) {
				t.Setenv("CSI_NODE_NAME", nodeName)
				getNodeMock, _ := getNodeMock(mockCtl, nodeName, &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key:    AgentNotReadyNodeTaintKey,
								Effect: corev1.TaintEffectNoSchedule,
							},
						},
					},
				}, nil)

				storageV1Mock := NewMockStorageV1Interface(mockCtl)
				getNodeMock.(*MockKubernetesClient).EXPECT().StorageV1().Return(storageV1Mock).AnyTimes()

				csiNodesMock := NewMockCSINodeInterface(mockCtl)
				storageV1Mock.EXPECT().CSINodes().Return(csiNodesMock).Times(1)

				mockCSINode := &v1.CSINode{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.CSINodeSpec{},
				}

				csiNodesMock.EXPECT().
					Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).
					Return(mockCSINode, nil).
					Times(1)

				return func() (kubernetes.Interface, error) {
					return getNodeMock, nil
				}
			},
			expResult: fmt.Errorf("isAllocatableSet: driver not found on node %s", nodeName),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()

			k8sClientGetter := tc.setup(t, mockCtl)
			client, err := k8sClientGetter()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			result := removeNotReadyTaint(client)

			if (result == nil) != (tc.expResult == nil) {
				t.Fatalf("expected %v, got %v", tc.expResult, result)
			}
			if result != nil && tc.expResult != nil {
				if result.Error() != tc.expResult.Error() {
					t.Fatalf("Expected error message `%v`, got `%v`", tc.expResult.Error(), result.Error())
				}
			}
		})
	}
}

func TestRemoveTaintInBackground(t *testing.T) {
	t.Run("Successful taint removal", func(t *testing.T) {
		mockRemovalCount := 0
		mockRemovalFunc := func(_ kubernetes.Interface) error {
			mockRemovalCount += 1
			if mockRemovalCount == 3 {
				return nil
			} else {
				return fmt.Errorf("Taint removal failed!")
			}
		}
		removeTaintInBackground(nil, taintRemovalBackoff, mockRemovalFunc)
		assert.Equal(t, 3, mockRemovalCount)
	})

	t.Run("Retries exhausted", func(t *testing.T) {
		mockRemovalCount := 0
		mockRemovalFunc := func(_ kubernetes.Interface) error {
			mockRemovalCount += 1
			return fmt.Errorf("Taint removal failed!")
		}
		removeTaintInBackground(nil, wait.Backoff{
			Steps:    5,
			Duration: 1 * time.Millisecond,
		}, mockRemovalFunc)
		assert.Equal(t, 5, mockRemovalCount)
	})
}

func getNodeMock(mockCtl *gomock.Controller, nodeName string, returnNode *corev1.Node, returnError error) (kubernetes.Interface, *MockNodeInterface) {
	mockClient := NewMockKubernetesClient(mockCtl)
	mockCoreV1 := NewMockCoreV1Interface(mockCtl)
	mockNode := NewMockNodeInterface(mockCtl)

	mockClient.EXPECT().CoreV1().Return(mockCoreV1).MinTimes(1)
	mockCoreV1.EXPECT().Nodes().Return(mockNode).MinTimes(1)
	mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(returnNode, returnError).MinTimes(1)

	return mockClient, mockNode
}

func expectErr(t *testing.T, actualErr error, expectedCode codes.Code) {
	if actualErr == nil {
		t.Fatalf("Expect error but got no error")
	}

	status, ok := status.FromError(actualErr)
	if !ok {
		t.Fatalf("Failed to get error status code from error: %v", actualErr)
	}

	if status.Code() != expectedCode {
		t.Fatalf("Expected error code %d, got %d message %s", codes.InvalidArgument, status.Code(), status.Message())
	}
}

type stubCryptMapper struct {
	CloseCryptDeviceErr  error
	OpenCryptDeviceErr   error
	ResizeCryptDeviceErr error
	GetDevicePathErr     error
}

func (s *stubCryptMapper) CloseCryptDevice(volumeID string) error {
	return s.CloseCryptDeviceErr
}

func (s *stubCryptMapper) OpenCryptDevice(ctx context.Context, source string, volumeID string, integrity bool) (string, error) {
	return "", s.OpenCryptDeviceErr
}

func (s *stubCryptMapper) ResizeCryptDevice(ctx context.Context, volumeID string) (string, error) {
	return "", s.ResizeCryptDeviceErr
}

func (s *stubCryptMapper) GetDevicePath(volumeID string) (string, error) {
	return "", s.GetDevicePathErr
}
